---
name: improve-coverage
description: Improve code coverage for a Bazel C++ test target or source file
user-invocable: true
---

# Improve Code Coverage

Improve test coverage for a Bazel C++ test target or source file path.

## Input Resolution

1. **Bazel test target** (starts with `//`, e.g. `//src/v/bytes/tests:iobuf_test`):
   use it directly.

2. **Source file path** (e.g. `src/v/bytes/iobuf.cc`): find the test target:
   ```bash
   bazel query "kind('.*_test', rdeps(//src/v/..., //$(dirname $FILE):$(basename $FILE .cc), 1))" 2>/dev/null
   ```
   If multiple targets match, pick the one in the closest `tests/` directory.
   If none match, tell the user and stop.

## Workflow

### Step 1: Baseline Coverage

Run coverage and read the LLM report:

```bash
tools/run-cov <target> -r llm
```

Note the **total line coverage percentage** — this is the baseline to beat.

Read the report and identify:
- **Uncovered functions** — highest impact. One test can cover many lines.
- **Partially covered functions** — uncovered branches indicate missing
  error-path or edge-case tests.
- **Zero-coverage files in scope** — may need new test cases entirely.

### Step 2: Understand the Code

Read the source files named in the report. Focus on:
- Uncovered functions: what they do, inputs, preconditions.
- The existing test file: patterns, fixtures, helpers.

Key codebase patterns:
- `redpanda_cc_gtest` targets use GTest (`TEST()`, `TEST_F()`).
  `redpanda_cc_btest` targets use Seastar Boost test
  (`SEASTAR_THREAD_TEST_CASE`). Match whichever the test file uses.
- Many functions are `ss::future<>` coroutines — test with
  `SEASTAR_THREAD_TEST_CASE` or the Seastar GTest runner.
- Use `ss` namespace prefix for Seastar types.
- Use `EXPECT_*` for most checks, `ASSERT_*` only when continuing would crash.

### Step 3: Write Tests

Prioritize by impact:
1. **Uncovered functions** — call each one. A 20-line function = 20 lines covered.
2. **Uncovered error paths** — trigger the uncovered branches in partially
   covered functions.
3. **Zero-coverage files** — add smoke tests if practical.

Rules:
- Add tests to the **existing test file**. Don't create new files or BUILD
  targets unless necessary.
- Follow existing naming conventions in the file.
- One logical behavior per test case.
- Prefer deterministic tests — construct error conditions directly.
- Do NOT add comments that restate the code.

### Step 4: Verify Build

```bash
bazel build <target>
```

Fix compilation errors before proceeding.

### Step 5: Verify Coverage

Re-run coverage:

```bash
tools/run-cov <target> -r llm
```

Compare to the baseline. Report:

```
Coverage: X.Y% → A.B% (+N.N%)
New lines covered: <count>
```

If coverage did not improve, investigate:
- Build errors preventing the new tests from running?
- Code compiled out by preprocessor guards?
- Template instantiations not triggered by test types?

### Step 6: Summary

Report:
- Which functions/paths are now covered
- What test cases were added
- Before/after coverage numbers
- Remaining significant gaps and suggestions
