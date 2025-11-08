#!/usr/bin/env bash

set -euo pipefail

image_name=python-type-check
tag=redpanda-data/$image_name

# Get the directory containing this script
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# redpanda repo
ROOT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Get the tests directory (go up two levels from tools/type-checking to root, then into tests)
TESTS_DIR="$ROOT_DIR/tests"
TC_DIR="$ROOT_DIR/tools/type-checking"

# Build the Docker image unless skipped
if [[ ${TC_SKIP_DOCKER_BUILD:-0} != "0" ]]; then
  echo "Skipping Docker build because TC_SKIP_DOCKER_BUILD=${TC_SKIP_DOCKER_BUILD}" >&2
else
  echo "Building Docker image $tag..."
  docker build -t $tag -f "$ROOT_DIR/tools/type-checking/Dockerfile" \
    ${TARGET:+--target=$TARGET} ${TC_DOCKER_ARGS-} "$ROOT_DIR/tests"
fi

# Run the container with the tests directory mounted
# echo "Running type checker in Docker container..."
docker run --rm -t \
  -v "$TESTS_DIR:$TESTS_DIR" -v "$TC_DIR:$TC_DIR" --entrypoint=$TC_DIR/type-check.py $tag \
  --no-venv --tests-root "$TESTS_DIR" "$@"
