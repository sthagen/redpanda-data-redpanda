#!/usr/bin/env bash
# Copyright 2026 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0
#
# Launch the DuckDB CLI pre-wired to the local Iceberg REST catalog and
# MinIO started by tools/dev_cluster.py. Starts in ~1s vs ~30s for
# tools/spark-sql.sh; full SQL (reads and writes) via DuckDB's iceberg
# extension.
#
# Override defaults via env, e.g.:
#   REST_URI=http://localhost:8181 S3_ENDPOINT=http://localhost:9000 \
#   WAREHOUSE=s3://panda-bucket/iceberg ./tools/duckdb.sh

set -euo pipefail

REST_URI="${REST_URI:-http://localhost:8181}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
WAREHOUSE="${WAREHOUSE:-s3://panda-bucket/iceberg}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_REGION="${S3_REGION:-panda-region}"
CATALOG_NAME="${CATALOG_NAME:-rp}"
NAMESPACE="${NAMESPACE:-redpanda}"

DUCKDB_VERSION="${DUCKDB_VERSION:-v1.5.3}"

DATA_DIR="${DATA_DIR:-${BUILD_WORKSPACE_DIRECTORY:-$PWD}/data}"
DUCKDB_DIR="$DATA_DIR/duckdb"
mkdir -p "$DUCKDB_DIR"

# DuckDB's S3 secret endpoint is host:port (no scheme).
s3_endpoint_host="${S3_ENDPOINT#http://}"
s3_endpoint_host="${s3_endpoint_host#https://}"
s3_use_ssl="false"
[[ $S3_ENDPOINT == https://* ]] && s3_use_ssl="true"

case "$(uname -m)" in
  x86_64) duckdb_asset="duckdb_cli-linux-amd64.zip" ;;
  aarch64) duckdb_asset="duckdb_cli-linux-arm64.zip" ;;
  *)
    echo "unsupported architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

DOWNLOAD_TMP=
INIT_SQL=
cleanup() {
  [[ -n $DOWNLOAD_TMP ]] && rm -rf "$DOWNLOAD_TMP"
  [[ -n $INIT_SQL ]] && rm -f "$INIT_SQL"
}
trap cleanup EXIT

DUCKDB_BIN="$DUCKDB_DIR/duckdb-$DUCKDB_VERSION"
if [[ ! -x $DUCKDB_BIN ]]; then
  echo "downloading DuckDB $DUCKDB_VERSION..." >&2
  DOWNLOAD_TMP=$(mktemp -d)
  curl -fsSL -o "$DOWNLOAD_TMP/duckdb.zip" \
    "https://github.com/duckdb/duckdb/releases/download/$DUCKDB_VERSION/$duckdb_asset"
  unzip -q "$DOWNLOAD_TMP/duckdb.zip" -d "$DOWNLOAD_TMP"
  mv "$DOWNLOAD_TMP/duckdb" "$DUCKDB_BIN"
fi

# Write the init script containing S3 credentials to a private temp file
# (mode 0600 via mktemp), rather than persisting secrets under $DATA_DIR.
INIT_SQL=$(mktemp)
cat >"$INIT_SQL" <<EOF
.bail on
INSTALL iceberg;
LOAD iceberg;

CREATE OR REPLACE SECRET s3_minio (
    TYPE s3,
    KEY_ID '$S3_ACCESS_KEY',
    SECRET '$S3_SECRET_KEY',
    ENDPOINT '$s3_endpoint_host',
    REGION '$S3_REGION',
    URL_STYLE 'path',
    USE_SSL $s3_use_ssl
);

ATTACH '$WAREHOUSE' AS $CATALOG_NAME (
    TYPE iceberg,
    ENDPOINT '$REST_URI',
    AUTHORIZATION_TYPE 'none',
    ACCESS_DELEGATION_MODE 'none'
);

USE $CATALOG_NAME.$NAMESPACE;
EOF

if [[ $# -gt 0 ]]; then
  "$DUCKDB_BIN" -init "$INIT_SQL" -c "$*"
else
  "$DUCKDB_BIN" -init "$INIT_SQL"
fi
