from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    explode,
    expr,
    lit,
    sha2,
    unix_timestamp,
)

CATALOG = "local"
SILVER_NS = "silver"
GOLD_NS = "gold"

SILVER_EVENTS = f"{CATALOG}.{SILVER_NS}.tfl_line_status_events"
CHECKPOINT = "s3a://lake/checkpoints/gold/tfl_line_status"

spark = SparkSession.builder.appName("tfl-line-status-gold-streaming").getOrCreate()
RUN_ID = spark.sparkContext.applicationId

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{GOLD_NS}")

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.fact_line_status_interval_stream (
  line_id STRING,
  line_name STRING,
  mode STRING,
  event_id STRING,
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,
  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  interval_seconds BIGINT,
  status_severity INT,
  status_desc STRING,
  is_good_service BOOLEAN,
  is_disrupted BOOLEAN,
  disruption_category STRING,
  reason STRING,
  reason_text_hash STRING,
  event_count_in_interval INT,
  source_batch_id BIGINT,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(valid_from), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.mart_line_hour_stream (
  bucket_hour TIMESTAMP,
  service_date DATE,
  line_id STRING,
  line_name STRING,
  mode STRING,
  total_seconds BIGINT,
  good_service_seconds BIGINT,
  disruption_seconds BIGINT,
  severity_weighted_seconds DOUBLE,
  incident_count BIGINT,
  source_batch_id BIGINT,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.current_line_status_stream (
  line_id STRING,
  line_name STRING,
  mode STRING,
  status_severity INT,
  status_desc STRING,
  is_disrupted BOOLEAN,
  disruption_category STRING,
  reason STRING,
  status_valid_from TIMESTAMP,
  status_valid_to TIMESTAMP,
  last_event_ts TIMESTAMP,
  last_ingest_ts TIMESTAMP,
  source_batch_id BIGINT,
  processed_at TIMESTAMP
)
USING iceberg
PARTITIONED BY (mode)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)


def merge_into_table(
    ss: SparkSession,
    df: DataFrame,
    table_fqn: str,
    on_clause: str,
    insert_cols: list[str],
    temp_prefix: str,
    run_id: str,
    batch_id: int,
    update_cols: list[str] | None = None,
):
    temp_view = f"{temp_prefix}_{run_id.replace('-', '_')}_{batch_id}"
    insert_sql = ", ".join(insert_cols)
    values_sql = ", ".join([f"s.{c}" for c in insert_cols])
    update_sql = ""
    if update_cols:
        assignments = ",\n              ".join([f"{c} = s.{c}" for c in update_cols])
        update_sql = f"\n            WHEN MATCHED THEN UPDATE SET\n              {assignments}"

    df.createOrReplaceTempView(temp_view)
    try:
        ss.sql(
            f"""
            MERGE INTO {table_fqn} t
            USING {temp_view} s
            ON {on_clause}{update_sql}
            WHEN NOT MATCHED THEN INSERT ({insert_sql})
            VALUES ({values_sql})
            """
        )
    finally:
        ss.catalog.dropTempView(temp_view)


def process_batch(silver_batch_df: DataFrame, batch_id: int):
    ss = silver_batch_df.sparkSession
    if silver_batch_df.rdd.isEmpty():
        return

    base = (
        silver_batch_df.select(
            "event_id",
            "event_ts",
            "ingest_ts",
            "line_id",
            "line_name",
            "mode",
            "status_severity",
            "status_desc",
            "reason",
            "valid_from",
            "valid_to",
            "is_disrupted",
        )
        .filter(col("line_id").isNotNull() & col("event_ts").isNotNull())
        .withColumn("valid_from", coalesce(col("valid_from"), col("event_ts")))
        .withColumn("valid_to", coalesce(col("valid_to"), current_timestamp()))
        .withColumn(
            "valid_to",
            expr(
                "CASE WHEN valid_to <= valid_from THEN valid_from + INTERVAL 1 SECOND ELSE valid_to END"
            ),
        )
        .withColumn("interval_seconds", unix_timestamp("valid_to") - unix_timestamp("valid_from"))
        .withColumn("is_good_service", expr("CASE WHEN status_severity >= 10 THEN true ELSE false END"))
        .withColumn(
            "is_disrupted",
            coalesce(col("is_disrupted"), expr("CASE WHEN status_severity >= 10 THEN false ELSE true END")),
        )
        .withColumn(
            "disruption_category",
            expr(
                """
                CASE
                  WHEN lower(coalesce(reason, '')) LIKE '%signal%' THEN 'Signal Failure'
                  WHEN lower(coalesce(reason, '')) LIKE '%train cancellation%' THEN 'Train Cancellations'
                  WHEN lower(coalesce(reason, '')) LIKE '%customer incident%' THEN 'Customer Incident'
                  WHEN lower(coalesce(reason, '')) LIKE '%power%' THEN 'Power Issue'
                  WHEN lower(coalesce(reason, '')) LIKE '%staff%' THEN 'Staffing'
                  WHEN lower(coalesce(reason, '')) LIKE '%weather%' THEN 'Weather'
                  WHEN lower(coalesce(reason, '')) LIKE '%engineering%' THEN 'Engineering Work'
                  WHEN lower(coalesce(reason, '')) LIKE '%planned closure%' THEN 'Planned Closure'
                  WHEN reason IS NULL OR trim(reason) = '' THEN 'Unknown'
                  ELSE 'Other'
                END
                """
            ),
        )
        .withColumn("reason_text_hash", sha2(coalesce(col("reason"), lit("")), 256))
        .withColumn("event_count_in_interval", lit(1).cast("int"))
        .withColumn("source_batch_id", lit(batch_id).cast("bigint"))
        .withColumn("processed_at", current_timestamp())
    )

    interval_cols = [
        "line_id",
        "line_name",
        "mode",
        "event_id",
        "event_ts",
        "ingest_ts",
        "valid_from",
        "valid_to",
        "interval_seconds",
        "status_severity",
        "status_desc",
        "is_good_service",
        "is_disrupted",
        "disruption_category",
        "reason",
        "reason_text_hash",
        "event_count_in_interval",
        "source_batch_id",
        "processed_at",
    ]

    merge_into_table(
        ss=ss,
        df=base.select(*interval_cols),
        table_fqn=f"{CATALOG}.{GOLD_NS}.fact_line_status_interval_stream",
        on_clause=(
            "t.event_id = s.event_id "
            "AND t.line_id = s.line_id "
            "AND t.status_severity <=> s.status_severity"
        ),
        insert_cols=interval_cols,
        temp_prefix="tmp_gold_interval_stream",
        run_id=RUN_ID,
        batch_id=batch_id,
    )

    hour_expanded = (
        base.withColumn(
            "bucket_hour",
            explode(
                expr(
                    "sequence(date_trunc('hour', valid_from), date_trunc('hour', valid_to), interval 1 hour)"
                )
            ),
        )
        .withColumn("overlap_start", expr("greatest(valid_from, bucket_hour)"))
        .withColumn("overlap_end", expr("least(valid_to, bucket_hour + interval 1 hour)"))
        .withColumn("overlap_seconds", unix_timestamp("overlap_end") - unix_timestamp("overlap_start"))
        .filter(col("overlap_seconds") > 0)
    )

    hour_metrics = (
        hour_expanded.groupBy("bucket_hour", "line_id")
        .agg(
            expr("max(line_name) as line_name"),
            expr("max(mode) as mode"),
            expr("sum(overlap_seconds) as total_seconds"),
            expr("sum(CASE WHEN is_good_service THEN overlap_seconds ELSE 0 END) as good_service_seconds"),
            expr("sum(CASE WHEN is_disrupted THEN overlap_seconds ELSE 0 END) as disruption_seconds"),
            expr(
                "sum(CASE "
                "WHEN status_severity >= 10 THEN 0.0 "
                "WHEN status_severity = 9 THEN overlap_seconds * 1.0 "
                "WHEN status_severity = 8 THEN overlap_seconds * 2.0 "
                "WHEN status_severity = 7 THEN overlap_seconds * 3.0 "
                "WHEN status_severity IN (6, 5) THEN overlap_seconds * 4.0 "
                "ELSE overlap_seconds * 2.0 END) as severity_weighted_seconds"
            ),
            expr("count_if(is_disrupted) as incident_count"),
        )
        .withColumn("service_date", expr("to_date(bucket_hour)"))
        .withColumn("source_batch_id", lit(batch_id).cast("bigint"))
        .withColumn("processed_at", current_timestamp())
    )

    hour_cols = [
        "bucket_hour",
        "service_date",
        "line_id",
        "line_name",
        "mode",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "incident_count",
        "source_batch_id",
        "processed_at",
    ]

    merge_into_table(
        ss=ss,
        df=hour_metrics.select(*hour_cols),
        table_fqn=f"{CATALOG}.{GOLD_NS}.mart_line_hour_stream",
        on_clause="t.bucket_hour = s.bucket_hour AND t.line_id = s.line_id",
        insert_cols=hour_cols,
        temp_prefix="tmp_gold_hour_stream",
        run_id=RUN_ID,
        batch_id=batch_id,
        update_cols=[
            "line_name",
            "mode",
            "total_seconds",
            "good_service_seconds",
            "disruption_seconds",
            "severity_weighted_seconds",
            "incident_count",
            "source_batch_id",
            "processed_at",
        ],
    )

    current = (
        base.groupBy("line_id")
        .agg(
            expr(
                "max_by(named_struct("
                "'line_name', line_name,"
                "'mode', mode,"
                "'status_severity', status_severity,"
                "'status_desc', status_desc,"
                "'is_disrupted', is_disrupted,"
                "'disruption_category', disruption_category,"
                "'reason', reason,"
                "'status_valid_from', valid_from,"
                "'status_valid_to', valid_to,"
                "'last_event_ts', event_ts,"
                "'last_ingest_ts', ingest_ts"
                "), event_ts) as latest_status"
            )
        )
        .select(
            col("line_id"),
            col("latest_status.line_name").alias("line_name"),
            col("latest_status.mode").alias("mode"),
            col("latest_status.status_severity").alias("status_severity"),
            col("latest_status.status_desc").alias("status_desc"),
            col("latest_status.is_disrupted").alias("is_disrupted"),
            col("latest_status.disruption_category").alias("disruption_category"),
            col("latest_status.reason").alias("reason"),
            col("latest_status.status_valid_from").alias("status_valid_from"),
            col("latest_status.status_valid_to").alias("status_valid_to"),
            col("latest_status.last_event_ts").alias("last_event_ts"),
            col("latest_status.last_ingest_ts").alias("last_ingest_ts"),
            lit(batch_id).cast("bigint").alias("source_batch_id"),
            current_timestamp().alias("processed_at"),
        )
    )

    current_cols = [
        "line_id",
        "line_name",
        "mode",
        "status_severity",
        "status_desc",
        "is_disrupted",
        "disruption_category",
        "reason",
        "status_valid_from",
        "status_valid_to",
        "last_event_ts",
        "last_ingest_ts",
        "source_batch_id",
        "processed_at",
    ]

    merge_into_table(
        ss=ss,
        df=current.select(*current_cols),
        table_fqn=f"{CATALOG}.{GOLD_NS}.current_line_status_stream",
        on_clause="t.line_id = s.line_id",
        insert_cols=current_cols,
        temp_prefix="tmp_gold_current_stream",
        run_id=RUN_ID,
        batch_id=batch_id,
        update_cols=[
            "line_name",
            "mode",
            "status_severity",
            "status_desc",
            "is_disrupted",
            "disruption_category",
            "reason",
            "status_valid_from",
            "status_valid_to",
            "last_event_ts",
            "last_ingest_ts",
            "source_batch_id",
            "processed_at",
        ],
    )


silver_stream = spark.readStream.table(SILVER_EVENTS)

query = (
    silver_stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
