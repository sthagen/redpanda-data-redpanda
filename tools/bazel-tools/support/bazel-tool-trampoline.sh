#!/usr/bin/env bash

# This script is a trampoline for bazel tools, allowing running
# of tools which are bazel targets, with:
# - minimal bazel output
# - the working directory set to the user's original working directory
# - avoiding a failure when bazel is run from the output directory
# - allowing the tool to be run from anywhere, even outside the workspace
#
# Use it by calling it with the target name as an environment variable:
#   BAZEL_TOOL_TRAMPOLINE_TARGET=//tools/bazel-tools:my_tool \
#   tools/bazel-tools/support/bazel-tool-trampoline.sh [args...]

set -euo pipefail

debug() {
  if [[ ${BAZEL_TRAMPOLINE_DEBUG:-0} -gt 0 ]]; then
    echo "DEBUG: bazel-tool-trampoline.sh:" "$@" >&2
  fi
}

# true in that symlinks are resolved
support_script_dir="$(cd -- "$(dirname -- "$(realpath "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

debug "support_script_dir: $support_script_dir"

if [[ -z ${BAZEL_TOOL_TRAMPOLINE_TARGET-} ]]; then
  # If BAZEL_TOOL_TRAMPOLINE_TARGET is not set, error and exit
  echo "ERROR: bazel-tool-trampoline.sh: BAZEL_TOOL_TRAMPOLINE_TARGET not set" >&2
  exit 1
fi

if [[ $BAZEL_TOOL_TRAMPOLINE_TARGET == *clang-tidy ]]; then
  if [[ -z ${BUILD_WORKSPACE_DIRECTORY-} ]]; then
    export BAZEL_BIN=$(bazel info bazel-bin)
  else
    export BAZEL_BIN="${BUILD_WORKSPACE_DIRECTORY}/bazel-bin"
  fi
  export LOADS=" -load ${BAZEL_BIN}/bazel/clang_tidy/plugins/plugins.so"
fi

export BAZEL_TOOL_TRAMPOLINE_CWD=$PWD

# switch to the directory containing this script so that bazel has the right
# context to be invoked (it just has to be anywhere under the workspace root)
if ! cd "$support_script_dir"; then
  echo "ERROR: bazel-tool-trampoline.sh: failed to cd to script directory" >&2
  exit 1
fi

debug "running $BAZEL_TOOL_TRAMPOLINE_TARGET"
debug "bazel invoke cwd: $(pwd)"
debug "target cwd      : $BAZEL_TOOL_TRAMPOLINE_CWD"

# If BAZEL_TRAMPOLINE_PARALLEL=1 use a separate output_base so this tool can run
# in parallel with the main bazel server without contending for the same lock.
# Best for tools that need to run frequently and don't need a ton of files in
# their output base. disk_cache will in general obviate the need to actually
# rebuild anything.
#
# Determining the default output_base is tricky: `bazel info output_base` itself
# takes the main server's lock, so if the main server is busy the query would
# block (defeating the point). Strategy (keyed on cache existence):
#   - No cache yet: no choice but to block — call `bazel info output_base` and
#     seed the cache with the result.
#   - Cache exists: try `bazel --noblock_for_lock info output_base` to
#     opportunistically refresh the cache; on lock-held (exit 9) just use the
#     cached value.
if [[ ${BAZEL_TRAMPOLINE_PARALLEL:-0} -gt 0 ]]; then
  cache_dir="$support_script_dir/../.cache"
  cache_file="$cache_dir/output_base"
  mkdir -p "$cache_dir"

  if [[ ! -f $cache_file ]]; then
    debug "output_base from: blocking bazel query (no cache yet)"
    default_output_base="$(bazel info output_base)"
    echo "$default_output_base" >"$cache_file"
  else
    # Bazel exits 9 (LOCK_HELD_NOBLOCK_FOR_LOCK) when --noblock_for_lock
    # cannot acquire the server lock. Any other non-zero exit is a real
    # error and should surface rather than silently falling back to cache.
    rc=0
    fresh=$(bazel --noblock_for_lock info output_base 2>/dev/null) || rc=$?
    if ((rc == 0)); then
      debug "output_base from: live bazel query (cache refreshed)"
      default_output_base=$fresh
      echo "$default_output_base" >"$cache_file"
    elif ((rc == 9)); then
      default_output_base="$(cat "$cache_file")"
      debug "output_base from: cache ($cache_file, lock held)"
    else
      echo "ERROR: bazel-tool-trampoline.sh: bazel info output_base failed (exit $rc)" >&2
      exit "$rc"
    fi
  fi

  tool_output_base="${default_output_base}_rptool"
  output_base_flag=(--output_base="$tool_output_base")

  debug "tool output_base   : $tool_output_base"
else
  output_base_flag=()
  debug "tool output_base   : (default)"
fi

# These flags are to hide a bunch of Bazel's built-in output.

exec bazel "${output_base_flag[@]}" \
  run "$BAZEL_TOOL_TRAMPOLINE_TARGET" \
  --run_under=//tools/bazel-tools/support:run \
  --noshow_progress \
  --ui_event_filters=,+error,+fail \
  --show_result=0 \
  --logging=0 \
  -- \
  ${LOADS-} \
  "$@"
