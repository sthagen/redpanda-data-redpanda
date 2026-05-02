#!/bin/bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
#
# Test for tools/run-cov coverage report generator.
#
# Receives positional args from Bazel:
#   $1 — path to the run-cov script
#   $2 — path to the LCOV fixture file
#   $3 — path to the diff fixture file

set -euo pipefail

RUN_COV="$1"
FIXTURE="$2"
DIFF_FIXTURE="$3"
TMPDIR="$(mktemp -d)"
trap 'rm -rf "$TMPDIR"' EXIT

fail() {
  echo "FAIL: $1" >&2
  exit 1
}

# ---------------------------------------------------------------
# 1. Terminal mode
# ---------------------------------------------------------------
output=$("$RUN_COV" --reuse --lcov "$FIXTURE" --no-color 2>&1)
echo "$output" | grep -q "Totals:" ||
  fail "terminal: missing Totals line"
echo "$output" | grep -q "src/v/utils/example.cc" ||
  fail "terminal: missing example.cc in output"

# ---------------------------------------------------------------
# 2. LLM mode
# ---------------------------------------------------------------
output=$("$RUN_COV" --reuse --lcov "$FIXTURE" -r llm --no-color 2>&1)
echo "$output" | grep -q "## Summary" ||
  fail "llm: missing Summary section"
echo "$output" | grep -q "## Uncovered Functions" ||
  fail "llm: missing Uncovered Functions section"
echo "$output" | grep -q "## Uncovered Line Ranges" ||
  fail "llm: missing Uncovered Line Ranges section"
echo "$output" | grep -q "## Files With Zero Coverage" ||
  fail "llm: missing Files With Zero Coverage section"

# ---------------------------------------------------------------
# 3. HTML mode (skip if genhtml not installed)
# ---------------------------------------------------------------
if command -v genhtml &>/dev/null; then
  "$RUN_COV" --reuse --lcov "$FIXTURE" -r html -o "$TMPDIR/html-out" 2>&1
  [ -f "$TMPDIR/html-out/html/index.html" ] ||
    fail "html: index.html not generated"
fi

# ---------------------------------------------------------------
# 4. Bad input: nonexistent LCOV file
# ---------------------------------------------------------------
if "$RUN_COV" --reuse --lcov /nonexistent/path.dat --no-color 2>/dev/null; then
  fail "should fail on nonexistent LCOV file"
fi

# ---------------------------------------------------------------
# 5. Missing target without --reuse
# ---------------------------------------------------------------
if "$RUN_COV" 2>/dev/null; then
  fail "should fail when no target and no --reuse"
fi

# ---------------------------------------------------------------
# 6. Diff coverage (terminal)
# ---------------------------------------------------------------
output=$("$RUN_COV" --reuse --lcov "$FIXTURE" --diff-file "$DIFF_FIXTURE" --no-color 2>&1)
echo "$output" | grep -q "Diff Coverage:" ||
  fail "diff terminal: missing Diff Coverage header"
echo "$output" | grep -q "Covered:" ||
  fail "diff terminal: missing Covered count"

# ---------------------------------------------------------------
# 7. Diff coverage (LLM)
# ---------------------------------------------------------------
output=$("$RUN_COV" --reuse --lcov "$FIXTURE" --diff-file "$DIFF_FIXTURE" -r llm --no-color 2>&1)
echo "$output" | grep -q "# Diff Coverage Report:" ||
  fail "diff llm: missing Diff Coverage Report header"
echo "$output" | grep -q "## Uncovered Changed Lines" ||
  fail "diff llm: missing Uncovered Changed Lines section"

echo "PASS: all run-cov tests passed"
