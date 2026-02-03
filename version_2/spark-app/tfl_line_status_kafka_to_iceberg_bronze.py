from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

KAFKA_BOOTSTRAP = "kafka:29092"  # internal listener on docker network
TOPIC = "tfl.raw.line_status"

CATALOG = "local"
NAMESPACE = "bronze"
TABLE = "tfl_raw_line_status"

CHECKPOINT = "s3a://lake/checkpoints/bronze/tfl_raw_line_status"


spark = SparkSession.builder.appName(
    "tfl-line-status-kafka-to-iceberg-bronze"
).getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{NAMESPACE}.{TABLE} (
  topic STRING,
  partition INT,
  offset BIGINT,
  kafka_timestamp TIMESTAMP,
  key STRING,
  value STRING,
  ingested_at TIMESTAMP
)
USING iceberg
"""
)

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "latest")
    .load()
)

bronze_df = kafka_df.select(
    col("topic").cast("string").alias("topic"),
    col("partition").cast("int").alias("partition"),
    col("offset").cast("long").alias("offset"),
    col("timestamp").alias("kafka_timestamp"),
    col("key").cast("string").alias("key"),
    col("value").cast("string").alias("value"),
    current_timestamp().alias("ingested_at"),
)


def write_batch(batch_df, batch_id: int):
    batch_df.writeTo(f"{CATALOG}.{NAMESPACE}.{TABLE}").append()


(
    bronze_df.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
    .awaitTermination()
)
