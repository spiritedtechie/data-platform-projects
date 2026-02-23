#!/usr/bin/env bash
set -euo pipefail

APP_PATH="${1:-}"

if [[ -z "${APP_PATH}" ]]; then
  echo "Usage: $0 <spark-app-path>"
  exit 1
fi

# Determine the path to spark-submit
if [[ -n "${SPARK_HOME:-}" && -x "${SPARK_HOME}/bin/spark-submit" ]]; then
  SPARK_SUBMIT="${SPARK_HOME}/bin/spark-submit"
elif command -v spark-submit >/dev/null 2>&1; then
  SPARK_SUBMIT="$(command -v spark-submit)"
elif [[ -x "/opt/spark/bin/spark-submit" ]]; then
  SPARK_SUBMIT="/opt/spark/bin/spark-submit"
else
  echo "spark-submit not found. Set SPARK_HOME or add spark-submit to PATH."
  exit 1
fi

# Submit the Spark job with the necessary configurations and dependencies
"${SPARK_SUBMIT}" \
  --master "local[*]" \
  --packages \
org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1,\
org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.10.1,\
org.apache.hadoop:hadoop-aws:3.4.2 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.defaultCatalog=local \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://lake/warehouse \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.connection.timeout=60000 \
  --conf spark.hadoop.fs.s3a.connection.establish.timeout=15000 \
  --conf spark.sql.streaming.maxOffsetsPerTrigger=5000 \
  "${APP_PATH}"