#!/usr/bin/env bash

set -eo pipefail

# What is this file?
# This allows us to inject external variables into our Bazel build.
# Care must be taken when modifying this, as incorrect usage could
# cause bazel to invalidate the cache too often.
#
# To RTFM, see: https://bazel.build/docs/user-manual#workspace-status-command
#
# Bazel only runs this when --config=stamp is used. At that point bazel invokes
# this script to generate key-value information that represents the status of the
# workspace. The output should be like
#
# KEY1 VALUE1
# KEY2 VALUE2
#
# If the script exits with non-zero code, the build will fail.
#
# Note that keys starting with "STABLE_" are part of the stable set, which if
# changed, invalidate any stampted targets (which by default is only binaries
# if the --stamp flag is passed to bazel, otherwise nothing). Keys which do
# not start with "STABLE_" are part of the volatile set, which will be used
# but do not invalidate stamped targets.

# In CI Bazel can sometimes be run from the vtools repo, so we need to ensure
# that we're in the correct redpanda git repo.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
WORKSPACE_DIR="${SCRIPT_DIR}/.."

git_tag=$(git -C "$WORKSPACE_DIR" describe --tags --always --abbrev=0 --match='v*')
echo "STABLE_GIT_LATEST_TAG ${git_tag}"

# For CI builds we don't want to use the commit hash as that prevents caching of binaries,
# ducktape generally only needs the tag anyways, so the hash we omit for everything except
# full release builds.
if [[ $1 != "full" ]]; then
  echo "STABLE_GIT_COMMIT 000000"
  echo "STABLE_GIT_TREE_DIRTY "
  exit 0
fi

git_rev=$(git -C "$WORKSPACE_DIR" rev-parse HEAD)
echo "STABLE_GIT_COMMIT ${git_rev}"

# Check whether there are any uncommitted changes
if git -C "$WORKSPACE_DIR" diff-index --quiet HEAD --; then
  echo "STABLE_GIT_TREE_DIRTY "
else
  echo "STABLE_GIT_TREE_DIRTY -dirty"
fi
