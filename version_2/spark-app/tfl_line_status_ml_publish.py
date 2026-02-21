from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp

CATALOG = "local"
GOLD_NS = "gold"
SCORES_BASE = "s3a://lake/ml/scores"

spark = SparkSession.builder.appName("tfl-line-status-ml-publish").getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{GOLD_NS}")

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_scores_line_hour (
  bucket_hour TIMESTAMP,
  line_id STRING,
  mode STRING,
  disruption_next_1h_probability DOUBLE,
  target INT,
  scored_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_forecast_line_day (
  service_date DATE,
  line_id STRING,
  mode STRING,
  predicted_downtime_next_day_minutes DOUBLE,
  target DOUBLE,
  scored_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (months(service_date), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_predicted_incident_duration (
  line_id STRING,
  mode STRING,
  incident_start_ts TIMESTAMP,
  status_severity INT,
  predicted_incident_duration_seconds DOUBLE,
  target DOUBLE,
  scored_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(incident_start_ts), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_alerts_line_hour (
  bucket_hour TIMESTAMP,
  line_id STRING,
  mode STRING,
  disruption_seconds BIGINT,
  expected_disruption_seconds DOUBLE,
  z_score DOUBLE,
  is_anomaly BOOLEAN,
  anomaly_type STRING,
  scored_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

ml1 = (
    spark.read.parquet(f"{SCORES_BASE}/ml_scores_line_hour.parquet")
    .withColumn("scored_at", current_timestamp())
)
ml1.writeTo(f"{CATALOG}.{GOLD_NS}.ml_scores_line_hour").overwritePartitions()

ml2 = (
    spark.read.parquet(f"{SCORES_BASE}/ml_forecast_line_day.parquet")
    .withColumn("scored_at", current_timestamp())
)
ml2.writeTo(f"{CATALOG}.{GOLD_NS}.ml_forecast_line_day").overwritePartitions()

ml3 = (
    spark.read.parquet(f"{SCORES_BASE}/ml_predicted_incident_duration.parquet")
    .withColumn("scored_at", current_timestamp())
)
ml3.writeTo(f"{CATALOG}.{GOLD_NS}.ml_predicted_incident_duration").overwritePartitions()

ml4 = (
    spark.read.parquet(f"{SCORES_BASE}/ml_alerts_line_hour.parquet")
    .select(
        col("bucket_hour"),
        col("line_id"),
        col("mode"),
        col("disruption_seconds"),
        col("expected_disruption_seconds"),
        col("z_score"),
        col("is_anomaly"),
        col("anomaly_type"),
    )
    .withColumn("scored_at", current_timestamp())
)
ml4.writeTo(f"{CATALOG}.{GOLD_NS}.ml_alerts_line_hour").overwritePartitions()

spark.stop()
