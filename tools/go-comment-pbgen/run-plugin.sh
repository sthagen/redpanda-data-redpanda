#!/bin/bash
# Wrapper script that runs the protoc-gen-comments plugin via `go run`
# This allows buf.gen.yaml to reference it without requiring a compiled binary

set -e

cd "$(dirname "$0")"
exec go run . "$@"
