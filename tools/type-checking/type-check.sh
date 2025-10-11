#!/usr/bin/env bash

set -euo pipefail

image_name=python-type-checking
tag=redpanda-data/$image_name

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# redpanda repo
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Get the tests directory (go up two levels from tools/type-checking to root, then into tests)
TESTS_DIR="$ROOT_DIR/tests"

# Build the Docker image
echo "Building Docker image $tag..."

# Create a tar stream with only the files needed by the Dockerfile COPY commands
# This is needed because we grab stuff from both tests/ and tools/ so the root
# would need to be the parent (repo root) and we don't want to send the whole repo
# context (this fails in practice too due to inaccessible files).
tar -C "$ROOT_DIR" -cf - \
  tests/setup.py \
  tools/type-checking |
  docker build -t $tag -f "tools/type-checking/Dockerfile" ${TARGET:+--target=$TARGET} -

# Run the container with the tests directory mounted
# echo "Running type checker in Docker container..."
docker run --rm -t \
  -v "$TESTS_DIR:$TESTS_DIR" $tag --no-venv --tests-root "$TESTS_DIR" "$@"
