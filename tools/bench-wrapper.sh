#!/usr/bin/env bash

# A wrapper which execs benchmarks after some prep.

set -euo pipefail

basename=$(basename "$1")
stderr=$(mktemp --tmpdir "$basename.stderr.XXXXXXXX")
exe_path=$(realpath "$1")
exec_in_shm=${MB_EXEC_IN_SHM:-1}
if [[ $exec_in_shm == 1 ]]; then
  rundir=${MB_RUNDIR:-/dev/shm/vectorized_io}
else
  rundir=${MB_RUNDIR:-.}
fi
# whether to redirect stderr (to avoid overwhelming the result output with
# seastar logging: use MB_REDIRECT_STDERR=1 in your env
# while calling bazel to override: bazel itself passes MB_REDIRECT_STDERR_DEFAULT
# based on how the benchmark is configured
redirect_stderr=${MB_REDIRECT_STDERR:-${MB_REDIRECT_STDERR_DEFAULT:-0}}
verbose=${MB_VERBOSE:-0}

# drop the relative exe path from the args
shift

echo "[bench-wrapper] running benchmark : $basename"
echo "[bench-wrapper] verbsose=$verbose, exec_in_shm=$exec_in_shm, redirect_stderr=$redirect_stderr"
if [[ $redirect_stderr == 1 ]]; then
  echo "[bench-wrapper] redirecting stderr: $stderr"
fi
echo "[bench-wrapper] stderr saved at   : $stderr"
echo "[bench-wrapper] rundir            : $rundir"
echo "[bench-wrapper] command           : ${exe_path} $*"

if [[ $verbose == 1 ]]; then
  echo "[bench-wrapper] env begin:"
  env
  echo "[bench-wrapper] env end"
fi

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
