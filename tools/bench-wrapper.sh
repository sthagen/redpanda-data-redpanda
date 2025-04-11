#!/usr/bin/env bash

# A wrapper which execs benchmarks after some prep.

set -euo pipefail

basename=$(basename "$1")
stderr=$(mktemp --tmpdir "$basename.stderr.XXXXXXXX")
rundir=${MB_RUNDIR:-/dev/shm/vectorized_io}
exe_path=$(realpath "$1")
# whether to redirect stderr (to avoid overwhelming the result output with
# seastar logging: use MB_REDIRECT_STDERR=1 in your env
# while calling bazel to override: bazel itself passes MB_REDIRECT_STDERR_DEFAULT
# based on how the benchmark is configured
redirect_stderr=${MB_REDIRECT_STDERR:-${MB_REDIRECT_STDERR_DEFAULT:-0}}

# drop the relative exe path from the args
shift

echo "[bench-wrapper] running benchmark : $basename"
if [[ $redirect_stderr == 1 ]]; then
  echo "[bench-wrapper] redirecting stderr: $stderr"
else
  echo "[bench-wrapper] not redirecting stderr"
fi
echo "[bench-wrapper] stderr saved at   : $stderr"
echo "[bench-wrapper] rundir            : $rundir"
echo "[bench-wrapper] command           : ${exe_path} $*"

mkdir -p "$rundir"
cd "$rundir"

rc=0
if [[ $redirect_stderr == 1 ]]; then
  $exe_path "$@" 2>$stderr || rc=$?
else
  $exe_path "$@" || rc=$?
fi

if [[ $rc -ne 0 ]]; then
  msg="[bench-wrapper] ERROR: benchmark failed (rc=$rc)"
  if [[ $redirect_stderr == 1 ]]; then
    echo "$msg first and last 40 lines of stderr below"
    echo "=== First 40 lines of stderr ==="
    head -n 40 "$stderr"
    echo "=== Last 40 lines of stderr ==="
    tail -n 40 "$stderr"
    echo "$msg, full stderr at $stderr"
  else
    echo "$msg"
  fi
  exit $rc
else
  echo "[bench-wrapper] benchmark completed successfully"
fi
