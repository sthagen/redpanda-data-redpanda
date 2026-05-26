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
# Launch spark-sql in Docker connected to the local Iceberg REST catalog
# started by tools/dev_cluster.py (defaults: REST on :8181, MinIO on :9000,
# warehouse s3://panda-bucket/iceberg).
#
# Override via env, e.g.:
#   REST_URI=http://localhost:8181 S3_ENDPOINT=http://localhost:9000 \
#   WAREHOUSE=s3://panda-bucket/iceberg ./tools/spark-sql.sh

set -euo pipefail

REST_URI="${REST_URI:-http://localhost:8181}"
S3_ENDPOINT="${S3_ENDPOINT:-http://localhost:9000}"
WAREHOUSE="${WAREHOUSE:-s3://panda-bucket/iceberg}"
S3_ACCESS_KEY="${S3_ACCESS_KEY:-minioadmin}"
S3_SECRET_KEY="${S3_SECRET_KEY:-minioadmin}"
S3_REGION="${S3_REGION:-panda-region}"
CATALOG_NAME="${CATALOG_NAME:-rp}"

SPARK_IMAGE="${SPARK_IMAGE:-apache/spark:4.0.2}"
ICEBERG_VERSION="${ICEBERG_VERSION:-1.10.1}"
ICEBERG_SPARK_RUNTIME="${ICEBERG_SPARK_RUNTIME:-iceberg-spark-runtime-4.0_2.13}"

# Match dev_cluster.py's data directory resolution so spark state (ivy/maven
# cache, hive history) lives alongside the cluster's data and persists across
# runs.
DATA_DIR="${DATA_DIR:-${BUILD_WORKSPACE_DIRECTORY:-$PWD}/data}"
SPARK_STATE_DIR="${SPARK_STATE_DIR:-$DATA_DIR/spark-home}"
mkdir -p "$SPARK_STATE_DIR"
# apache/spark runs as uid 185 (spark) with HOME=/nonexistent; make the
# mount writable without --user, which breaks Hadoop's JAAS unix login.
# Only the top-level dir needs to be world-writable — files created inside
# by the container are already owned by uid 185 and accessible to it.
chmod 777 "$SPARK_STATE_DIR"

# spark.sql.catalogImplementation=in-memory skips Hive metastore (Derby)
# init — unused, and otherwise emits warnings.
# spark.ui.enabled=false skips the Web UI on :4040 — interactive REPL
# doesn't need it.
exec docker run --rm -it \
  --network host \
  -v "$SPARK_STATE_DIR:/home/spark" \
  "$SPARK_IMAGE" \
  /opt/spark/bin/spark-sql \
  --driver-java-options "-Duser.home=/home/spark" \
  --conf spark.jars.ivy=/home/spark/.ivy2 \
  --packages "org.apache.iceberg:${ICEBERG_SPARK_RUNTIME}:${ICEBERG_VERSION},org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VERSION}" \
  --conf spark.sql.catalogImplementation=in-memory \
  --conf spark.ui.enabled=false \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf "spark.sql.catalog.${CATALOG_NAME}=org.apache.iceberg.spark.SparkCatalog" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.type=rest" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.cache-enabled=false" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.uri=${REST_URI}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.warehouse=${WAREHOUSE}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.io-impl=org.apache.iceberg.aws.s3.S3FileIO" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.endpoint=${S3_ENDPOINT}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.path-style-access=true" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.access-key-id=${S3_ACCESS_KEY}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.s3.secret-access-key=${S3_SECRET_KEY}" \
  --conf "spark.sql.catalog.${CATALOG_NAME}.client.region=${S3_REGION}" \
  --conf "spark.sql.defaultCatalog=${CATALOG_NAME}"
