#!/usr/bin/env bash

# This script it to used as a target for --run_under= when using bazel run.
# It changes the working directory to BUILD_WORKING_DIRECTORY, which is the
# user's working directory when they invoked bazel: otherwise, bazel will
# run with the runfiles directory as the working directory, which would
# break things like file paths passed to the tool.

set -euo pipefail

# https://github.com/bazelbuild/bazel/issues/3325#issuecomment-2046157439

if [[ -n ${BAZEL_TOOL_TRAMPOLINE_CWD-} ]]; then
  target_cwd="$BAZEL_TOOL_TRAMPOLINE_CWD"
  from=BAZEL_TOOL_TRAMPOLINE_CWD
elif [[ -n ${BUILD_WORKING_DIRECTORY-} ]]; then
  target_cwd="$BUILD_WORKING_DIRECTORY"
  from=BUILD_WORKING_DIRECTORY
else
  echo "ERROR: run.sh: one of BAZEL_TOOL_TRAMPOLINE_CWD or BUILD_WORKING_DIRECTORY must be set." >&2
  exit 1
fi

if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
  echo "DEBUG: run.sh: target cwd (from: $from): $target_cwd" >&2
  echo "DEBUG: run.sh: full command: $*" >&2
fi

if ! cd "$target_cwd"; then
  echo "ERROR: run.sh: cd to BUILD_WORKING_DIRECTORY failed: '$target_cwd'" >&2
  exit 1
fi

exec "$@"
