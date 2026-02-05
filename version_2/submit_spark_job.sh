#!/usr/bin/env bash
set -euo pipefail

# Ensure the docker containers are running via docker-compose i.e. `docker-compose up -d`

APP_PATH="${1}"

docker exec -it spark bash -lc "
  /opt/spark/bin/spark-submit \
    --packages \
org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,\
org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,\
org.apache.hadoop:hadoop-aws:3.3.4,\
com.amazonaws:aws-java-sdk-bundle:1.12.262 \
    --conf spark.jars.ivy=/tmp/.ivy2 \
    --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
    --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
    --conf spark.sql.catalog.local.type=hadoop \
    --conf spark.sql.catalog.local.warehouse=s3a://lake/warehouse \
    --conf spark.hadoop.fs.s3a.endpoint=http://minio:9000 \
    --conf spark.hadoop.fs.s3a.access.key=minioadmin \
    --conf spark.hadoop.fs.s3a.secret.key=minioadmin123 \
    --conf spark.hadoop.fs.s3a.path.style.access=true \
    --conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
    --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
    --conf spark.driver.memory=512m \
    --conf spark.executor.memory=512m \
    --conf spark.executor.cores=1 \
    --conf spark.sql.shuffle.partitions=6 \
    --conf spark.default.parallelism=6 \
    --conf spark.sql.streaming.maxOffsetsPerTrigger=5000 \
    ${APP_PATH}
"