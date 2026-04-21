#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Variant of clang-tool.sh that runs the tool against a separate bazel
# output_base so it can execute in parallel with the main bazel server.
# Only suitable for tools that don't depend on bazel-built artifacts
# (e.g. clang-tidy plugins) living in the default output_base.

export BAZEL_TRAMPOLINE_PARALLEL="${BAZEL_TRAMPOLINE_PARALLEL:-1}"

tools_dir="$(cd -- "$(dirname -- "$(realpath "${BASH_SOURCE[0]}")")" &>/dev/null && pwd)"

# `exec -a` doesn't propagate through a shebang into bash's $0, so forward
# the invocation name via an env var that clang-tool.sh consults.
export CLANG_TOOL_NAME="$(basename "$0")"
exec "$tools_dir/clang-tool.sh" "$@"
