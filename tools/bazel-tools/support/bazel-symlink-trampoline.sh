#!/usr/bin/env bash

# This script is a trampoline for Bazel tools, allowing symlinked scripts to
# be executed with a calculated target name based on the script name.
# Specifically, create a symlink named run-<target> to this script,
# and it will do a bazel run on //tools/bazel-tools/<target>, using
# the bazel-tool-trampoline.sh script (see that script for details).

set -euo pipefail

script_name=$(basename "$0")
support_script_dir="$(cd -- "$(dirname -- "$(realpath "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
  echo "DEBUG: bazel-symlink-trampoline.sh: script_name: $script_name, support_script_dir: $support_script_dir" >&2
fi

# Calculate target name based on removing run- from the script name
if [[ $script_name =~ ^run-(.+)$ ]]; then
  export BAZEL_TOOL_TRAMPOLINE_TARGET="//tools/bazel-tools:${BASH_REMATCH[1]}"
else
  echo "ERROR: bazel-symlink-trampoline.sh: script name does not start with run-" >&2
  exit 1
fi

if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
  echo "DEBUG: calculated target: $BAZEL_TOOL_TRAMPOLINE_TARGET" >&2
fi

# Call the main trampoline logic
exec "$support_script_dir/bazel-tool-trampoline.sh" "$@"
