# Copyright 2025 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import os
import subprocess
import sys
from pathlib import Path


def main():
    if len(sys.argv) < 2:
        print(
            "Usage: bench_wrapper.py <benchmark_executable> [args...]", file=sys.stderr
        )
        sys.exit(1)

    def getenv_default(var: str, default: int = 0):
        return int(os.getenv(var, os.getenv(var + "_DEFAULT", str(default))))

    redirect_stderr = getenv_default("MB_REDIRECT_STDERR")
    verbose = getenv_default("MB_VERBOSE")
    disable_aslr = getenv_default("MB_DISABLE_ASLR", 1)
    exec_in_shm = getenv_default("MB_EXEC_IN_SHM", 1)

    exe = Path(sys.argv[1]).resolve()

    exe_prefix = ["setarch", "--addr-no-randomize"] if disable_aslr else []

    rundir = Path(
        os.getenv("MB_RUNDIR", "/dev/shm/vectorized_io" if exec_in_shm else ".")
    )

    # Resolve environment variables we pass to our benchmarks to absolute paths
    # this way we don't have errors
    for k in ["ASAN_SYMBOLIZER_PATH"]:
        v = os.getenv(k, None)
        if not v:
            continue
        os.environ[k] = str(Path(v).resolve())
    # Resolve suppressions files as well
    for opt_name in ["UBSAN_OPTIONS", "LSAN_OPTIONS", "ASAN_OPTIONS"]:
        options = os.getenv(opt_name, None)
        if not options:
            continue
        options = options.split(":")
        resolved_options: list[str] = []
        for option in options:
            k, v = option.split("=", maxsplit=1)
            if k == "suppressions":
                v = str(Path(v).resolve())
            resolved_options.append(f"{k}={v}")
        os.environ[opt_name] = ":".join(resolved_options)
    # This is sort of horrible, but this sets the include search directory for openssl configuration file.
    # In CMake we used an absolute path for the .include (which is what OpenSSL recommends), but since
    # Bazel tries *really* hard to use relative paths for better caching, we need to add the current path
    # to the include path for the openssl configuration.
    # See: https://docs.openssl.org/3.1/man5/config/#environment
    os.environ["OPENSSL_CONF_INCLUDE"] = str(Path().resolve())

    print(f"[bench-wrapper] running benchmark : {exe.name}", file=sys.stderr)
    print(
        f"[bench-wrapper] verbose={verbose}, exec_in_shm={exec_in_shm}, redirect_stderr={redirect_stderr}, disable_aslr={disable_aslr}",
        file=sys.stderr,
    )
    print(f"[bench-wrapper] rundir            : {rundir}", file=sys.stderr)
    print(
        f"[bench-wrapper] command           : {' '.join(sys.argv[1:])}", file=sys.stderr
    )

    def printenv(name: str):
        v = os.getenv(name, None)
        v = "(unset)" if v is None else v
        print(f"{name}={v} ", file=sys.stderr, end="")

    print("[bench-wrapper] ", end="", file=sys.stderr)
    printenv("REDPANDA_RNG_SEEDING_MODE")
    printenv("REDPANDA_RNG_SEEDING_MODE_DEFAULT")
    print(file=sys.stderr)

    if verbose != 0:
        print("[bench-wrapper] env begin:", file=sys.stderr)
        for k, v in os.environ.items():
            print(f"{k}={v}", file=sys.stderr)
        print("[bench-wrapper] env end", file=sys.stderr)

    rundir.mkdir(parents=True, exist_ok=True)
    proc = subprocess.run(
        exe_prefix + [exe] + sys.argv[2:],
        stderr=subprocess.PIPE if redirect_stderr else None,
        text=True,
        cwd=rundir,
    )
    if proc.returncode != 0:
        msg = f"[bench-wrapper] ERROR: benchmark failed (rc={proc.returncode})"
        print(msg, file=sys.stderr)
        if proc.stderr:
            print(proc.stderr, file=sys.stderr)
    else:
        print("[bench-wrapper] benchmark completed successfully", file=sys.stderr)
    sys.exit(proc.returncode)


if __name__ == "__main__":
    main()
