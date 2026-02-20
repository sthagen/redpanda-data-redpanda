#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

# Pre-commit hook script: verify that every *newly added* code file contains
# the copyright notice with current year somewhere in its first 2 lines.

set -euo pipefail

YEAR=$(date +%Y)

# Get the list of files staged as Added (not modified/renamed/etc).
while IFS= read -r -d '' file; do
  if ! head -n 2 "$file" | grep -qF "Copyright $YEAR Redpanda Data, Inc."; then
    echo "$file: first 2 lines do not contain the copyright note with current year ($YEAR)"
    exit 1
  fi
done < <(git diff --cached --name-only -z --diff-filter=A -- '*.cc' '*.h' '*.py' '*.go' '*.java' '*.sh')
