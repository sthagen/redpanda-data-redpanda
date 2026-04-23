---
name: create-backport-branch
description: "Create a new backport branch with prefix `ai-backport-` by cherry-picking all commits from a GitHub PR onto a target release branch or commit. Use when the user wants to backport a PR, create a backport branch, cherry-pick PR commits to a release branch, or mentions backporting changes from dev/main to a version branch (e.g. v25.1.x, v25.2.x). Requires two arguments: TARGET (the release branch or commit hash to backport onto) and PR_NUM (the GitHub PR number to backport)."
argument-hint: [target-branch-or-commit] [pr-number]
allowed-tools: Bash(git *), Bash(gh *), Bash(date *), Bash(grep *), Bash(wc *), Read, Write, Edit, Glob, Grep, Agent
model: opus[1m]
effort: high
---

# Create Backport Branch

Cherry-pick all commits from a GitHub PR onto a target release branch, resolving conflicts along the way.

Arguments:
- `$0` is target — a release branch (e.g. `v25.1.x`) or a commit hash (e.g. `81d9e1af33`)
- `$1` is pr-number (e.g. `12345`)

## Procedure

### 1. Validate inputs and set up

1. Both arguments must be provided. If either target-branch or pr-number is empty, abort with: "Usage: /create-backport-branch <target-branch> <pr-number>"
2. Generate the backport-branch-name using the current Unix timestamp:
   ```bash
   date +%s
   ```
   backport-branch-name format: `ai-backport-pr-${pr_number}-${target_branch}-${unix_timestamp}`

### 2. Create the backport branch

```bash
git checkout -b $backport_branch_name $target_branch
```

### 3. Fetch PR commits

Get the list of commit SHAs from the PR:

```bash
gh api "repos/{owner}/{repo}/pulls/$pr_number/commits" --paginate --jq '[.[].sha[0:10]]|join(" ")'
```

`gh` resolves the `{owner}` and `{repo}` placeholders from the git remote of the current directory (see [`gh api` docs](https://cli.github.com/manual/gh_api)). Run this from a checkout of the repo that owns the PR.

Save the full list of commits as `git_commits` and report how many commits were found.

### 4. Cherry-pick commits

Run the cherry-pick for all commits at once:

```bash
git cherry-pick -x $git_commits
```

Track these counters as you go:
- **total_commits**: number of commits from the PR
- **conflicts_resolved**: number of commits that needed conflict resolution
- **commits_skipped**: number of commits skipped because they were already present on the target branch
- **generated_files_touched**: list of generated files that were part of the cherry-picked commits (whether conflicted or not)

If the cherry-pick succeeds cleanly, check if any generated files were touched (see step 4a), then skip to step 6.

### 4a. Check for generated files in cherry-picked commits

After each successful cherry-pick (clean or after conflict resolution), check if any generated files were touched. Use the same generated file patterns from step 5.3. For clean cherry-picks, check the files changed in the commit(s) just applied:

```bash
git diff --name-only HEAD~1 HEAD
```

Match the output against the generated file patterns. Add any matches to `generated_files_touched`.

### 5. Resolve conflicts

If the cherry-pick fails with merge conflicts, attempt resolution. For each conflicted state:

1. Identify the commit that caused the conflict (the one currently being cherry-picked).
2. List conflicted files: `git diff --name-only --diff-filter=U`
3. Categorize each file:
   - **Generated files** (accept theirs and move on): `*.pb.go`, `*.pb.h`, `*.pb.cc`, `*.pb.rs`, `*_pb2.py`, `go.sum`, `*.lock`, `MODULE.bazel.lock`, `package-lock.json`, `Cargo.lock`, `MODULE.bazel`
     - For generated files: `git checkout --theirs -- $FILE && git add -- $FILE`
     - Add each generated file to the `generated_files_touched` list
   - **Modify/delete conflicts**: file deleted on one side but modified on the other. These require human judgment — **abort immediately** (see abort procedure below).
   - **Content conflicts**: files with `<<<<<<<` / `=======` / `>>>>>>>` markers. Attempt resolution.

4. For each content conflict:
   a. Read the full conflicted file
   b. Run `git log -p -1 $FAILING_COMMIT -- $FILE` to understand what the cherry-picked commit intended
   c. Understand both sides:
      - **HEAD side** (`<<<<<<<` to `=======`): what the target release branch has
      - **Incoming side** (`=======` to `>>>>>>>`): what the cherry-picked commit brings
   d. Read surrounding code if needed to understand context (imports, types, function signatures)
   e. Determine the correct resolution:
      - The goal is to apply the *intent* of the cherry-picked commit to the target branch
      - Adapt to the target branch's patterns, names, and APIs where they diverge
   f. Edit the file to remove all conflict markers and write the correct merged code
   g. Stage: `git add -- $FILE`

5. Verify the resolution is clean:
   ```bash
   grep -rn '<<<<<<< \|=======$\|>>>>>>> ' -- $RESOLVED_FILES
   git diff --name-only --diff-filter=U
   ```
   Both commands should produce no output. If unmerged paths remain, something was missed.

6. Continue the cherry-pick:
   ```bash
   git cherry-pick --continue --no-edit
   ```
   If `--continue` reports "The previous cherry-pick is now empty", the commit's changes are already on the target branch. Run `git cherry-pick --skip` to move on, and increment `commits_skipped`.

7. Increment `conflicts_resolved` counter.

8. If the next commit also conflicts, repeat from step 5.1.

### 5a. Empty cherry-picks (no conflict)

A cherry-pick can also be empty without any conflict — git will report "The previous cherry-pick is now empty" immediately. This happens when the commit's changes already exist on the target branch (e.g., from a prior backport or parallel fix). Run `git cherry-pick --skip` to move on and increment `commits_skipped`. This is normal and not an error.

**Abort procedure** — If at any point you are uncertain how to resolve a conflict (ambiguous intent, modify/delete conflicts, architectural changes you don't understand, or anything that feels risky):

1. Run `git cherry-pick --abort`
2. Report an error with:
   - The exact `git cherry-pick -x ...` command that was being attempted
   - The specific commit SHA that caused the uncertain conflict
   - Which files were conflicted and why you couldn't resolve them
3. Stop immediately. Do NOT attempt further cherry-picks.

### 6. Final report

After all commits are cherry-picked successfully, resolve the source-PR URL dynamically:

```bash
pr_url=$(gh pr view $pr_number --json url --jq .url)
```

Then emit this report to stdout:

```
Backport of PR ${pr_url}

- Command: git cherry-pick -x ${git_commits}
- Commits backported: ${total_commits}
- Conflicts resolved: ${conflicts_resolved}
- Commits skipped (already on target): ${commits_skipped}
- Backport branch: ${backport_branch_name}

## Conflict details

- ${commit_sha} (${file}): one-line explanation of what conflicted and how it was resolved
```

For each conflict resolved, include a one-line explanation covering what conflicted and how you resolved it. This helps PR reviewers understand the backport without reading every diff.

If `generated_files_touched` is non-empty, append this warning section:

```
## ⚠️ Generated files

The following files were cherry-picked and may need regeneration:

- ${file_1}
- ${file_2}

These files were accepted as-is from the source branch. Before merging,
regenerate them on the target branch to ensure they're correct. For example:
- MODULE.bazel.lock: run `bazel mod deps --lockfile_mode=update`
- *.pb.go / *.pb.cc / *.pb.h: rebuild protobuf targets
- go.sum: run `go mod tidy`
```

**Optional: machine-readable report.** If the `SKILL_REPORT_FILE` env var is set (callers like GitHub Actions workflows use this), write the exact same report text to that file path. The file content is designed to be usable verbatim as the body of the backport PR. Humans invoking the skill directly do not set this variable and simply read the report on stdout.

## Rules

- NEVER guess at conflict resolutions. If uncertain, abort immediately using the abort procedure.
- NEVER silently drop code. Incoming changes that add new functionality must be preserved (adapted to the target branch's conventions).
- Do NOT add comments to resolved code unless the original commit had them.
- Preserve the formatting style of the target branch.
- For generated files (protobuf, lockfiles, MODULE.bazel), just accept theirs — they'll be regenerated by the build.
