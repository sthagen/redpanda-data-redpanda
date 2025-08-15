#!/usr/bin/env bash

# Hide a bunch of Bazel's built in output.
# So it's as close to a proxy for a clang tool
# executable as possible. Just symlink this file
# to the name of the clang tool you want.

script_name=$(basename "$0")

bazel run "@current_llvm_toolchain_llvm//:bin/$script_name" \
  --noshow_progress \
  --ui_event_filters=,+error,+fail \
  --show_result=0 \
  --logging=0 \
  -- $@
