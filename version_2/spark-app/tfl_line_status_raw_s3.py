from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_date, date_format

KAFKA_BOOTSTRAP = "kafka:29092"  # internal listener on docker network
TOPIC = "tfl.raw.line_status"

RAW_PATH = "s3a://lake/raw/tfl_line_status"
CHECKPOINT = "s3a://lake/checkpoints/raw/tfl_line_status"

spark = SparkSession.builder.appName("tfl-line-status-raw").getOrCreate()

kafka_df = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .option("failOnDataLoss", "false")
    .load()
)

raw_df = (
    kafka_df.withColumn("ingest_ts", current_timestamp())
    .withColumn("ingest_date", to_date("ingest_ts"))
    .withColumn("ingest_hour", date_format("ingest_ts", "HH"))
    .select(
        col("topic").alias("kafka_topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_ts"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("payload"),
        "ingest_ts",
        "ingest_date",
        "ingest_hour",
    )
)

(
    raw_df.writeStream.format("parquet")
    .option("path", RAW_PATH)
    .option("checkpointLocation", CHECKPOINT)
    .partitionBy("ingest_date", "ingest_hour")
    .outputMode("append")
    .trigger(processingTime="30 seconds")
    .start()
    .awaitTermination()
)
