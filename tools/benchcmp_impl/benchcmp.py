#!/usr/bin/env python3
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

"""Compare a Seastar microbenchmark run against a saved baseline.

While optimizing a hot path you repeat one loop: measure, edit, measure, and
ask whether the change helped or was just noise. This tool answers that in a
single command:

    tools/benchcmp record //src/v/utils/tests:coro_rpbench
    # ...edit...
    tools/benchcmp run //src/v/utils/tests:coro_rpbench

`record` runs the benchmark and saves the result, keyed by target and tagged
with the commit it was measured at. `run` runs the benchmark again and prints a
per-case diff with an improved, regressed, or noise verdict.

The tool takes care of two details. It builds with `--config=release`, so the
numbers come from an optimized binary. And it passes an absolute `--json-output`
path, because the benchmark wrapper runs the binary from /dev/shm; a relative
path would write the results there, where we could not read them back. Baselines
are stored under the XDG cache directory, not in the repository.

The verdict is decided by one metric, instructions retired (`inst`) by default.
`inst` is nearly deterministic once ASLR and random-number seeding are fixed
(the benchmark macro does both) and does not vary with CPU frequency, which makes
it far less noisy than wall-clock time. Choose a different metric with `--metric`
(`runtime`, `allocs`, or `tasks`); the rest are still shown for reference. If `inst`
is requested but the host has no hardware perf counters, benchcmp stops rather
than fall back to a noisier metric.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import subprocess
import sys
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

# `inst` is nearly deterministic, so even a small relative change is meaningful.
INST_EPS = 0.005  # 0.5%
# A runtime change counts only if the two medians differ by more than this many
# combined MADs...
MAD_K = 3.0
# ...and by at least this fraction, so a tiny change never flips a verdict even
# when the MAD is very small.
RUNTIME_FLOOR = 0.02  # 2%

SUBCOMMANDS = ("record", "run", "report")


@dataclass
class CaseMetrics:
    median: float  # nanoseconds
    mad: float  # nanoseconds
    allocs: float
    tasks: float
    inst: float
    cycles: float
    runs: float


@dataclass
class GitInfo:
    sha: str
    dirty: bool
    subject: str


def workspace_root() -> Path:
    """The repo root. `bazel run` sets BUILD_WORKSPACE_DIRECTORY for us."""
    env = os.environ.get("BUILD_WORKSPACE_DIRECTORY")
    if env:
        return Path(env)
    out = subprocess.run(
        ["git", "rev-parse", "--show-toplevel"], capture_output=True, text=True
    )
    return Path(out.stdout.strip()) if out.returncode == 0 else Path.cwd()


def git_info(root: Path) -> GitInfo:
    def git(*args: str) -> str:
        return subprocess.run(
            ["git", "-C", str(root), *args], capture_output=True, text=True
        ).stdout.strip()

    return GitInfo(
        sha=git("rev-parse", "--short", "HEAD") or "unknown",
        dirty=bool(git("status", "--porcelain")),
        subject=git("log", "-1", "--pretty=%s"),
    )


def baseline_dir(root: Path) -> Path:
    """Directory where baselines are stored, under the XDG cache directory. The
    path includes a hash of the workspace, so separate clones keep separate
    baselines."""
    xdg = os.environ.get("XDG_CACHE_HOME")
    base = Path(xdg) if xdg else Path.home() / ".cache"
    ws_hash = hashlib.sha256(str(root.resolve()).encode()).hexdigest()[:16]
    d = base / "benchcmp" / ws_hash
    d.mkdir(parents=True, exist_ok=True)
    return d


def baseline_path(root: Path, target: str) -> Path:
    safe = re.sub(r"[^A-Za-z0-9]+", "_", target).strip("_")
    return baseline_dir(root) / f"{safe}.json"


def parse_results(doc: dict) -> dict[str, CaseMetrics]:
    out: dict[str, CaseMetrics] = {}
    for name, m in doc.get("results", {}).items():
        out[name] = CaseMetrics(
            median=float(m.get("median", 0.0)),
            mad=float(m.get("mad", 0.0)),
            allocs=float(m.get("allocs", 0.0)),
            tasks=float(m.get("tasks", 0.0)),
            inst=float(m.get("inst", 0.0)),
            cycles=float(m.get("cycles", 0.0)),
            runs=float(m.get("runs", 0.0)),
        )
    return out


def run_benchmark(
    root: Path,
    target: str,
    runs: int | None,
    duration: float | None,
    tests: str | None,
    passthrough: list[str],
) -> dict:
    """Run the benchmark with `bazel run --config=release` and return its parsed
    JSON. The output path is absolute because the wrapper runs the binary from
    /dev/shm, where a relative path would write the file out of our reach."""
    fd, tmp = tempfile.mkstemp(prefix="benchcmp_", suffix=".json")
    os.close(fd)
    json_path = Path(tmp).resolve()

    cmd = [
        "bazel",
        "run",
        "--config=release",
        target,
        "--",
        "--json-output",
        str(json_path),
        "--no-stdout",
    ]
    # Forward these only when set: some targets define runs/duration in their
    # BUILD, and passing them a second time makes the option parser fail.
    if runs is not None:
        cmd.append(f"--runs={runs}")
    if duration is not None:
        cmd.append(f"--duration={duration}")
    if tests:
        cmd += ["-t", tests]
    cmd += passthrough

    err(paint("running", "bold") + f" {target} " + paint("(--config=release)", "dim"))
    proc = subprocess.run(cmd, cwd=str(root))
    if proc.returncode != 0:
        err(paint(f"benchmark failed (exit {proc.returncode})", "red"))
        json_path.unlink(missing_ok=True)
        sys.exit(proc.returncode)
    if not json_path.exists():
        err(paint(f"benchmark produced no results at {json_path}", "red"))
        sys.exit(1)
    doc = json.loads(json_path.read_text())
    json_path.unlink(missing_ok=True)
    return doc


# --- comparison ------------------------------------------------------------

Direction = Literal["improve", "regress", "flat"]
Verdict = Literal["improved", "regressed", "noise"]
Metric = Literal["inst", "runtime", "allocs", "tasks"]


@dataclass
class CaseDiff:
    verdict: Verdict
    inst_ok: bool
    inst_pct: float
    inst_sig: bool
    rt_pct: float
    rt_sig: bool


def _direction(improve: bool, regress: bool) -> Direction:
    if improve:
        return "improve"
    if regress:
        return "regress"
    return "flat"


def diff_case(base: CaseMetrics, cur: CaseMetrics, metric: Metric) -> CaseDiff:
    inst_ok = base.inst > 0 and cur.inst > 0
    inst_pct = (cur.inst - base.inst) / base.inst if (inst_ok and base.inst) else 0.0
    inst_sig = inst_ok and abs(inst_pct) >= INST_EPS

    rt_pct = (cur.median - base.median) / base.median if base.median else 0.0
    rt_sig = (
        abs(cur.median - base.median) > MAD_K * (base.mad + cur.mad)
        and abs(rt_pct) >= RUNTIME_FLOOR
    )

    direction = _primary_direction(
        metric, base, cur, inst_pct, inst_sig, rt_pct, rt_sig
    )
    verdict: dict[Direction, Verdict] = {
        "improve": "improved",
        "regress": "regressed",
        "flat": "noise",
    }
    return CaseDiff(
        verdict=verdict[direction],
        inst_ok=inst_ok,
        inst_pct=inst_pct,
        inst_sig=inst_sig,
        rt_pct=rt_pct,
        rt_sig=rt_sig,
    )


def _primary_direction(
    metric: Metric,
    base: CaseMetrics,
    cur: CaseMetrics,
    inst_pct: float,
    inst_sig: bool,
    rt_pct: float,
    rt_sig: bool,
) -> Direction:
    # The chosen metric alone decides the verdict; the other metrics are shown for
    # reference. Counts (allocs, tasks) are exact, so any change in them counts.
    if metric == "inst":
        return _direction(inst_sig and inst_pct < 0, inst_sig and inst_pct > 0)
    if metric == "runtime":
        return _direction(rt_sig and rt_pct < 0, rt_sig and rt_pct > 0)
    if metric == "allocs":
        delta = round(cur.allocs) - round(base.allocs)
    else:  # tasks
        delta = round(cur.tasks) - round(base.tasks)
    return _direction(delta < 0, delta > 0)


# --- terminal output -------------------------------------------------------

# Color is used only on an interactive terminal, and never when NO_COLOR is set.
_COLOR = sys.stdout.isatty() and os.environ.get("NO_COLOR") is None
_CODES = {
    "green": "\033[32m",
    "red": "\033[31m",
    "yellow": "\033[33m",
    "cyan": "\033[36m",
    "dim": "\033[2m",
    "bold": "\033[1m",
}
_RESET = "\033[0m"
_ANSI_RE = re.compile(r"\033\[[0-9;]*m")


def paint(text: str, *styles: str) -> str:
    if not _COLOR or not styles:
        return text
    return "".join(_CODES[s] for s in styles) + text + _RESET


def _visible_len(s: str) -> int:
    return len(_ANSI_RE.sub("", s))


def out(line: str = "") -> None:
    print(line)


def err(line: str = "") -> None:
    print(line, file=sys.stderr)


def print_table(headers: list[str], rows: list[list[str]], right: set[int]) -> None:
    """Print an aligned table. `right` holds the indices of right-justified
    columns. Cells may contain color codes; widths are measured without them."""
    widths = [_visible_len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            widths[i] = max(widths[i], _visible_len(cell))

    def fmt(cells: list[str]) -> str:
        out_cells = []
        for i, cell in enumerate(cells):
            pad = " " * (widths[i] - _visible_len(cell))
            out_cells.append(pad + cell if i in right else cell + pad)
        return "  ".join(out_cells).rstrip()

    out(fmt([paint(h, "bold") for h in headers]))
    out(paint("─" * (sum(widths) + 2 * (len(widths) - 1)), "dim"))
    for row in rows:
        out(fmt(row))


# --- formatting ------------------------------------------------------------

VERDICTS: dict[Verdict, tuple[str, tuple[str, ...]]] = {
    "improved": ("✓ improved", ("bold", "green")),
    "regressed": ("✗ regressed", ("bold", "red")),
    "noise": ("≈ noise", ("dim",)),
}

_UNITS = (("ns", 1.0), ("µs", 1e3), ("ms", 1e6), ("s", 1e9))
_DASH = paint("—", "dim")

_METRICS_HEADERS = ["case", "inst", "runtime med±mad", "allocs", "tasks"]
_NUMERIC_COLUMNS = {1, 2, 3, 4}


def fmt_count(n: float) -> str:
    for unit, div in (("G", 1e9), ("M", 1e6), ("K", 1e3)):
        if abs(n) >= div:
            return f"{n / div:.2f}{unit}"
    return f"{n:.0f}"


def fmt_runtime(median: float, mad: float) -> str:
    unit, div = _UNITS[0]
    for unit, div in _UNITS:
        if median < div * 1000:
            break
    prec = 0 if div == 1.0 else 2
    return f"{median / div:.{prec}f}±{mad / div:.{prec}f}{unit}"


def delta_markup(
    pct: float, significant: bool, lower_is_better: bool = True, gating: bool = True
) -> str:
    if not significant:
        return paint(f"{pct:+.1f}% =", "dim")
    arrow = "▼" if pct < 0 else "▲"
    # A non-gating delta (a metric that is not the chosen metric) is dimmed so the
    # table's colors always match the verdict.
    if not gating:
        return paint(f"{pct:+.1f}% {arrow}", "dim")
    improved = (pct < 0) == lower_is_better
    return paint(f"{pct:+.1f}% {arrow}", "green" if improved else "red")


def count_cell(
    base: float, cur: float, lower_is_better: bool = True, gating: bool = True
) -> str:
    cur_i = int(round(cur))
    if int(round(base)) == cur_i:
        return f"{cur_i} {paint('=', 'dim')}"
    delta = cur_i - int(round(base))
    arrow = "▼" if delta < 0 else "▲"
    if not gating:
        return f"{cur_i} {paint(f'{delta:+d} {arrow}', 'dim')}"
    improved = (delta < 0) == lower_is_better
    return f"{cur_i} {paint(f'{delta:+d} {arrow}', 'green' if improved else 'red')}"


def _runs_of(metrics: dict[str, CaseMetrics]) -> int:
    for m in metrics.values():
        if m.runs:
            return int(m.runs)
    return 0


def _inst_cell(c: CaseMetrics) -> str:
    return fmt_count(c.inst) if c.inst else _DASH


def _metrics_rows(metrics: dict[str, CaseMetrics]) -> list[list[str]]:
    return [
        [
            name,
            _inst_cell(c),
            fmt_runtime(c.median, c.mad),
            f"{int(c.allocs)}",
            f"{int(c.tasks)}",
        ]
        for name, c in sorted(metrics.items())
    ]


# --- rendering -------------------------------------------------------------


def _print_target_header(target: str) -> None:
    out(f"\n{paint(target, 'bold')}  {paint('· --config=release', 'dim')}")


def _print_run_line(
    label: str, git: GitInfo, runs: int, opts: argparse.Namespace
) -> None:
    dirty = paint("dirty", "red") if git.dirty else paint("clean", "green")
    extra = f"runs={runs}"
    if opts.duration is not None:
        extra += f" d={opts.duration}s"
    if opts.tests:
        extra += f" -t {opts.tests}"
    subject = ""
    if git.subject and label == "baseline":
        subject = "  " + paint(f'"{git.subject}"', "dim")
    head = paint(label.ljust(9), "cyan")
    out(f"  {head} {git.sha} ({dirty}){subject}  {paint(extra, 'dim')}")


def render_comparison(
    target: str,
    base_meta: dict,
    cur_git: GitInfo,
    base: dict[str, CaseMetrics],
    cur: dict[str, CaseMetrics],
    opts: argparse.Namespace,
    metric: Metric,
) -> None:
    _print_target_header(target)
    base_git = GitInfo(
        sha=base_meta.get("sha", "?"),
        dirty=bool(base_meta.get("dirty")),
        subject=base_meta.get("subject", ""),
    )
    base_opts = argparse.Namespace(
        duration=base_meta.get("duration"), tests=base_meta.get("tests")
    )
    _print_run_line("baseline", base_git, _runs_of(base), base_opts)
    _print_run_line("current", cur_git, _runs_of(cur), opts)
    out()

    headers = ["case", "inst (Δ)", "runtime med±mad (Δ)", "allocs", "tasks", "verdict"]
    rows: list[list[str]] = []
    counts: dict[str, int] = {}
    for name, c in sorted(cur.items()):
        if name not in base:
            rows.append(
                [
                    name,
                    _inst_cell(c),
                    fmt_runtime(c.median, c.mad),
                    f"{int(c.allocs)}",
                    f"{int(c.tasks)}",
                    paint("— no baseline", "dim"),
                ]
            )
            counts["unmatched"] = counts.get("unmatched", 0) + 1
            continue
        d = diff_case(base[name], c, metric)
        counts[d.verdict] = counts.get(d.verdict, 0) + 1
        # The chosen metric's delta is colored; the others are dimmed, so the
        # table's colors always match the verdict.
        if d.inst_ok:
            inst_delta = delta_markup(
                d.inst_pct * 100, d.inst_sig, gating=metric == "inst"
            )
            inst_cell = f"{fmt_count(c.inst)}  {inst_delta}"
        else:
            inst_cell = _DASH
        rt_cell = (
            f"{fmt_runtime(c.median, c.mad)}  "
            f"{delta_markup(d.rt_pct * 100, d.rt_sig, gating=metric == 'runtime')}"
        )
        label, styles = VERDICTS[d.verdict]
        rows.append(
            [
                name,
                inst_cell,
                rt_cell,
                count_cell(base[name].allocs, c.allocs, gating=metric == "allocs"),
                count_cell(base[name].tasks, c.tasks, gating=metric == "tasks"),
                paint(label, *styles),
            ]
        )

    print_table(headers, rows, _NUMERIC_COLUMNS)

    order = ["improved", "regressed", "noise", "unmatched"]
    parts = [f"{counts[k]} {k}" for k in order if counts.get(k)]
    out("  " + " · ".join(parts) + "      " + paint(f"primary metric: {metric}", "dim"))

    missing = sorted(set(base) - set(cur))
    if missing:
        out(
            "  "
            + paint(
                f"{len(missing)} baseline case(s) not measured this run: "
                f"{', '.join(missing)}",
                "dim",
            )
        )
    out()


def render_metrics(
    target: str,
    label: str,
    git: GitInfo,
    metrics: dict[str, CaseMetrics],
    opts: argparse.Namespace,
    note: str | None = None,
) -> None:
    _print_target_header(target)
    _print_run_line(label, git, _runs_of(metrics), opts)
    if note:
        out("  " + paint(note, "dim"))
    out()
    print_table(_METRICS_HEADERS, _metrics_rows(metrics), _NUMERIC_COLUMNS)
    out()


def render_no_baseline(
    target: str, cur_git: GitInfo, cur: dict[str, CaseMetrics], opts: argparse.Namespace
) -> None:
    _print_target_header(target)
    record = f"tools/benchcmp record {target}"
    head = paint("baseline".ljust(9), "cyan")
    out(f"  {head} {paint('none yet', 'yellow')} — record with: {paint(record, 'dim')}")
    _print_run_line("current", cur_git, _runs_of(cur), opts)
    out()
    print_table(_METRICS_HEADERS, _metrics_rows(cur), _NUMERIC_COLUMNS)
    out()


# --- commands --------------------------------------------------------------


def cmd_record(root: Path, opts: argparse.Namespace, passthrough: list[str]) -> None:
    doc = run_benchmark(
        root, opts.target, opts.runs, opts.duration, opts.tests, passthrough
    )
    git = git_info(root)
    meta = {
        "sha": git.sha,
        "dirty": git.dirty,
        "subject": git.subject,
        "tests": opts.tests,
        "duration": opts.duration,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    }
    path = baseline_path(root, opts.target)
    path.write_text(
        json.dumps({"meta": meta, "results": doc.get("results", {})}, indent=2)
    )
    err(
        paint("recorded baseline", "green")
        + f" for {opts.target} @ {git.sha}{' (dirty)' if git.dirty else ''}"
    )
    render_metrics(opts.target, "baseline", git, parse_results(doc), opts)


def cmd_run(root: Path, opts: argparse.Namespace, passthrough: list[str]) -> None:
    path = baseline_path(root, opts.target)
    doc = run_benchmark(
        root, opts.target, opts.runs, opts.duration, opts.tests, passthrough
    )
    cur = parse_results(doc)
    cur_git = git_info(root)
    if not path.exists():
        render_no_baseline(opts.target, cur_git, cur, opts)
        return
    saved = json.loads(path.read_text())
    base = parse_results({"results": saved.get("results", {})})
    if opts.metric == "inst" and not (
        any(m.inst for m in cur.values()) and any(m.inst for m in base.values())
    ):
        err(
            paint("error:", "red")
            + " instructions-retired counters are unavailable (inst=0); "
            "re-run with --metric runtime (or allocs/tasks)."
        )
        sys.exit(1)
    render_comparison(
        opts.target, saved.get("meta", {}), cur_git, base, cur, opts, opts.metric
    )


def cmd_report(root: Path, opts: argparse.Namespace, passthrough: list[str]) -> None:
    path = baseline_path(root, opts.target)
    if not path.exists():
        err(paint("no baseline recorded for", "yellow") + f" {opts.target}")
        return
    saved = json.loads(path.read_text())
    meta = saved.get("meta", {})
    base = parse_results({"results": saved.get("results", {})})
    git = GitInfo(
        meta.get("sha", "?"), bool(meta.get("dirty")), meta.get("subject", "")
    )
    meta_opts = argparse.Namespace(
        duration=meta.get("duration"), tests=meta.get("tests")
    )
    render_metrics(
        opts.target,
        "baseline",
        git,
        base,
        meta_opts,
        note=f"recorded {meta.get('timestamp', '?')}",
    )


HELP = """\
benchcmp — compare a Seastar microbenchmark against a saved baseline.

usage:
  tools/benchcmp run <target> [options]     run + diff against the baseline
  tools/benchcmp record <target> [options]  record a baseline
  tools/benchcmp report <target>            print the stored baseline

options:
  -t, --tests REGEX     only run cases matching REGEX (seastar -t)
  -r, --runs N          number of runs (default: benchmark's own default)
  -d, --duration SECS   seconds per run (default: benchmark's own default)
  --metric METRIC       metric that decides the verdict: inst (default), runtime,
                        allocs, tasks
  -- <args>             pass remaining args straight to the benchmark binary

examples:
  tools/benchcmp record //src/v/utils/tests:coro_rpbench
  tools/benchcmp run //src/v/utils/tests:coro_rpbench -t 'spawn.*' -r 11
"""


def main() -> None:
    # Force UTF-8 so the table glyphs print regardless of the ambient locale.
    for stream in (sys.stdout, sys.stderr):
        reconfigure = getattr(stream, "reconfigure", None)
        if reconfigure:
            reconfigure(encoding="utf-8")

    argv = sys.argv[1:]
    if not argv or argv[0] in ("-h", "--help"):
        print(HELP)
        return

    if "--" in argv:
        i = argv.index("--")
        head, passthrough = argv[:i], argv[i + 1 :]
    else:
        head, passthrough = argv, []

    parser = argparse.ArgumentParser(prog="benchcmp", add_help=True)
    parser.add_argument("command", choices=SUBCOMMANDS)
    parser.add_argument("target", help="fully-qualified bazel _rpbench target")
    parser.add_argument("-t", "--tests", default=None)
    parser.add_argument("-r", "--runs", type=int, default=None)
    parser.add_argument("-d", "--duration", type=float, default=None)
    parser.add_argument(
        "--metric",
        choices=("inst", "runtime", "allocs", "tasks"),
        default="inst",
        help="metric that decides the verdict (default: inst)",
    )
    opts = parser.parse_args(head)

    root = workspace_root()
    {"record": cmd_record, "run": cmd_run, "report": cmd_report}[opts.command](
        root, opts, passthrough
    )


if __name__ == "__main__":
    main()
