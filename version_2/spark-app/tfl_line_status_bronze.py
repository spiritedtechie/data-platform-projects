from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, sha2

KAFKA_BOOTSTRAP = "localhost:9092"  # internal listener on docker network
TOPIC = "tfl.raw.line_status"

CATALOG = "local"
NAMESPACE = "bronze"
TABLE = "tfl_line_status"

CHECKPOINT = "s3a://lake/checkpoints/bronze/tfl_line_status"


spark = SparkSession.builder.appName("tfl-line-status-bronze").getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.{TABLE} (
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_ts TIMESTAMP,
  kafka_key STRING,
  payload STRING,
  ingest_ts TIMESTAMP,
  payload_hash STRING
)
USING iceberg
PARTITIONED BY (days(ingest_ts))
TBLPROPERTIES (
  'format-version'='2',
  'write.format.default'='parquet'
)
"""
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)

bronze_df = (
    kafka_df.select(
        col("topic").cast("string").alias("kafka_topic"),
        col("partition").cast("int").alias("kafka_partition"),
        col("offset").cast("long").alias("kafka_offset"),
        col("timestamp").alias("kafka_ts"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("payload"),
    )
    .withColumn("ingest_ts", current_timestamp())
    .withColumn("payload_hash", sha2(col("payload"), 256))
)


# Write to Iceberg (append-only bronze)
(
    bronze_df.writeStream.format("iceberg")
    .outputMode("append")
    .option("checkpointLocation", CHECKPOINT)
    .toTable(f"{CATALOG}.{NAMESPACE}.{TABLE}")
    .awaitTermination()
)
