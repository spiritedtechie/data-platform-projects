from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    current_timestamp,
    from_json,
    explode_outer,
    coalesce,
    to_timestamp,
    expr,
    sha2,
    concat_ws,
    count as fcount,
    sum as fsum,
    min as fmin,
    max as fmax,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
)

# ----------------------------
# Config
# ----------------------------
CATALOG = "local"

BRONZE_NS = "bronze"
BRONZE_TABLE = "tfl_raw_line_status"
BRONZE_FQN = f"{CATALOG}.{BRONZE_NS}.{BRONZE_TABLE}"

SILVER_NS = "silver"
OPS_NS = "ops"
QUAR_NS = "quarantine"

CHECKPOINT = "s3a://lake/checkpoints/silver/tfl_bronze_to_silver"

# ----------------------------
# Spark session (Iceberg extensions configured in spark-submit)
# ----------------------------
spark = SparkSession.builder.appName("tfl-line-status-bronze-to-silver").getOrCreate()

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{OPS_NS}")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{QUAR_NS}")

# ----------------------------
# Target tables
# ----------------------------
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_NS}.tfl_line_status_events (
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,

  line_id STRING,
  line_name STRING,
  mode STRING,

  status_id BIGINT,
  status_severity INT,
  status_desc STRING,
  reason STRING,

  valid_from TIMESTAMP,
  valid_to TIMESTAMP,
  is_now BOOLEAN,

  raw_request_id STRING,
  raw_payload_hash STRING,

  bronze_topic STRING,
  bronze_partition INT,
  bronze_offset BIGINT,
  schema_version INT
)
USING iceberg
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_NS}.tfl_disruption_events (
  event_time TIMESTAMP,
  ingested_at TIMESTAMP,

  line_id STRING,
  disruption_id STRING,

  category STRING,
  category_desc STRING,
  closure_text STRING,
  description STRING,

  raw_request_id STRING,
  raw_payload_hash STRING,

  bronze_topic STRING,
  bronze_partition INT,
  bronze_offset BIGINT,
  schema_version INT
)
USING iceberg
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{QUAR_NS}.tfl_bad_records (
  ingested_at TIMESTAMP,
  bronze_topic STRING,
  bronze_partition INT,
  bronze_offset BIGINT,
  key STRING,
  value STRING,
  error_reason STRING,
  measured_at TIMESTAMP
)
USING iceberg
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{OPS_NS}.dq_metrics (
  batch_id BIGINT,
  dataset STRING,

  total_rows BIGINT,
  good_rows BIGINT,
  bad_rows BIGINT,

  null_line_id BIGINT,
  null_status_severity BIGINT,
  parse_fail_rows BIGINT,

  min_event_time TIMESTAMP,
  max_event_time TIMESTAMP,

  measured_at TIMESTAMP
)
USING iceberg
"""
)

# ----------------------------
# Schemas for parsing JSON payload (define all nested structs/arrays)
# ----------------------------
validity_schema = StructType(
    [
        StructField("fromDate", StringType(), True),
        StructField("toDate", StringType(), True),
        StructField("isNow", StringType(), True),  # sometimes boolean; keep string-safe
    ]
)

disruption_schema = StructType(
    [
        StructField("category", StringType(), True),
        StructField("categoryDescription", StringType(), True),
        StructField("closureText", StringType(), True),
        StructField("description", StringType(), True),
    ]
)

line_status_schema = StructType(
    [
        StructField("id", LongType(), True),
        StructField("lineId", StringType(), True),
        StructField("statusSeverity", IntegerType(), True),
        StructField("statusSeverityDescription", StringType(), True),
        StructField("reason", StringType(), True),
        StructField("created", StringType(), True),
        StructField(
            "disruption", disruption_schema, True
        ),  # single struct in your samples
        StructField("validityPeriods", ArrayType(validity_schema), True),
    ]
)

line_schema = StructType(
    [
        StructField("id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("modeName", StringType(), True),
        StructField("created", StringType(), True),
        StructField("modified", StringType(), True),
        StructField("lineStatuses", ArrayType(line_status_schema), True),
    ]
)

envelope_schema = StructType(
    [
        StructField("request_id", StringType(), True),
        StructField("ingested_at", StringType(), True),  # ISO string from your producer
        StructField("payload_hash", StringType(), True),
        StructField("schema_version", IntegerType(), True),
        StructField("payload", ArrayType(line_schema), True),
    ]
)


# ----------------------------
# Batch transform (optimized): operates ONLY on the micro-batch DF
# ----------------------------
def process_batch(bronze_batch_df: DataFrame, batch_id: int):
    measured_at = current_timestamp()

    # Expect bronze columns: topic, partition, offset, kafka_timestamp, key, value, ingested_at
    b = bronze_batch_df.select(
        col("topic").alias("bronze_topic"),
        col("partition").cast("int").alias("bronze_partition"),
        col("offset").cast("bigint").alias("bronze_offset"),
        col("kafka_timestamp"),
        col("key").cast("string").alias("key"),
        col("value").cast("string").alias("value"),
        col("ingested_at").cast("timestamp").alias("bronze_ingested_at"),
    )

    # Parse envelope JSON
    p = b.withColumn("env", from_json(col("value"), envelope_schema))

    # Quarantine parse failures
    parse_fail = p.filter(col("env").isNull()).select(
        col("bronze_ingested_at").alias("ingested_at"),
        col("bronze_topic"),
        col("bronze_partition"),
        col("bronze_offset"),
        col("key"),
        col("value"),
        lit("JSON_PARSE_FAILED").alias("error_reason"),
        measured_at.alias("measured_at"),
    )

    good_env = p.filter(col("env").isNotNull())

    # Envelope fields + robust event_time fallback
    # (We’ll later prefer validityPeriods.fromDate when present)
    env_norm = (
        good_env.withColumn("raw_request_id", col("env.request_id"))
        .withColumn("raw_payload_hash", col("env.payload_hash"))
        .withColumn("schema_version", coalesce(col("env.schema_version"), lit(1)))
        .withColumn("env_ingested_ts", to_timestamp(col("env.ingested_at")))
        .withColumn(
            "base_event_time",
            coalesce(
                col("env_ingested_ts"),
                col("kafka_timestamp"),
                col("bronze_ingested_at"),
            ),
        )
    )

    # Explode lines and statuses
    lines = env_norm.withColumn("line", explode_outer(col("env.payload")))

    statuses = lines.withColumn(
        "status", explode_outer(col("line.lineStatuses"))
    ).select(
        col("base_event_time"),
        col("bronze_ingested_at").alias("ingested_at"),
        col("line.id").alias("line_id"),
        col("line.name").alias("line_name"),
        col("line.modeName").alias("mode"),
        col("status.id").cast("bigint").alias("status_id"),
        col("status.statusSeverity").alias("status_severity"),
        col("status.statusSeverityDescription").alias("status_desc"),
        col("status.reason").alias("reason"),
        col("status.validityPeriods").alias("validity_periods"),
        col("status.disruption").alias("disruption"),
        col("raw_request_id"),
        col("raw_payload_hash"),
        col("bronze_topic"),
        col("bronze_partition"),
        col("bronze_offset"),
        col("schema_version"),
    )

    # Derive valid_from / valid_to / is_now from validityPeriods
    # Choose "isNow=true" period if present, else first period.
    # Handle isNow as string/bool by normalizing to string.
    statuses = (
        statuses.withColumn(
            "vp_now",
            expr(
                "filter(validity_periods, x -> lower(cast(x.isNow as string)) = 'true')"
            ),
        )
        .withColumn(
            "vp_pick",
            expr(
                "CASE WHEN size(vp_now) > 0 THEN vp_now[0] WHEN size(validity_periods) > 0 THEN validity_periods[0] ELSE NULL END"
            ),
        )
        .withColumn("valid_from", to_timestamp(col("vp_pick.fromDate")))
        .withColumn("valid_to", to_timestamp(col("vp_pick.toDate")))
        .withColumn(
            "is_now",
            expr(
                "CASE WHEN vp_pick IS NULL THEN false ELSE lower(cast(vp_pick.isNow as string)) = 'true' END"
            ),
        )
        .withColumn(
            # Prefer valid_from for event_time if present
            "event_time",
            coalesce(col("valid_from"), col("base_event_time")),
        )
    )

    # Required fields (for silver status events)
    good_status = statuses.filter(
        col("line_id").isNotNull() & col("status_severity").isNotNull()
    )
    bad_status = statuses.filter(
        col("line_id").isNull() | col("status_severity").isNull()
    )

    # Quarantine missing required fields (keep original raw message)
    # Join back to original batch rows via (topic,partition,offset) to store raw value for debugging
    bad_required = (
        bad_status.select("bronze_topic", "bronze_partition", "bronze_offset")
        .distinct()
        .join(b, on=["bronze_topic", "bronze_partition", "bronze_offset"], how="left")
        .select(
            col("bronze_ingested_at").alias("ingested_at"),
            col("bronze_topic"),
            col("bronze_partition"),
            col("bronze_offset"),
            col("key"),
            col("value"),
            lit("REQUIRED_FIELD_MISSING").alias("error_reason"),
            measured_at.alias("measured_at"),
        )
    )

    # Silver: line status events
    status_events_out = good_status.select(
        col("event_time"),
        col("ingested_at"),
        col("line_id"),
        col("line_name"),
        col("mode"),
        col("status_id"),
        col("status_severity"),
        col("status_desc"),
        col("reason"),
        col("valid_from"),
        col("valid_to"),
        col("is_now"),
        col("raw_request_id"),
        col("raw_payload_hash"),
        col("bronze_topic"),
        col("bronze_partition"),
        col("bronze_offset"),
        col("schema_version"),
    )

    # Silver: disruption events (only where disruption is present)
    disruption_events_out = (
        statuses.filter(col("disruption").isNotNull())
        .withColumn("category", col("disruption.category"))
        .withColumn("category_desc", col("disruption.categoryDescription"))
        .withColumn("closure_text", col("disruption.closureText"))
        .withColumn("description", col("disruption.description"))
        .withColumn(
            "disruption_id",
            sha2(
                concat_ws(
                    "||",
                    col("line_id"),
                    col("category"),
                    col("closure_text"),
                    col("description"),
                    col("event_time").cast("string"),
                ),
                256,
            ),
        )
        .select(
            col("event_time"),
            col("ingested_at"),
            col("line_id"),
            col("disruption_id"),
            col("category"),
            col("category_desc"),
            col("closure_text"),
            col("description"),
            col("raw_request_id"),
            col("raw_payload_hash"),
            col("bronze_topic"),
            col("bronze_partition"),
            col("bronze_offset"),
            col("schema_version"),
        )
    )

    # ----------------------------
    # Write outputs (append)
    # ----------------------------
    # Quarantine first (so you can debug even if silver fails later)
    parse_fail.writeTo(f"{CATALOG}.{QUAR_NS}.tfl_bad_records").append()
    bad_required.writeTo(f"{CATALOG}.{QUAR_NS}.tfl_bad_records").append()

    status_events_out.writeTo(f"{CATALOG}.{SILVER_NS}.tfl_line_status_events").append()
    disruption_events_out.writeTo(
        f"{CATALOG}.{SILVER_NS}.tfl_disruption_events"
    ).append()

    # ----------------------------
    # DQ metrics (single-pass aggregations)
    # ----------------------------
    # Count parse fails
    parse_fail_cnt = parse_fail.agg(fcount(lit(1)).alias("c")).collect()[0]["c"]

    # Metrics for status events
    agg = statuses.agg(
        fcount(lit(1)).alias("total_rows"),
        fsum(expr("CASE WHEN line_id IS NULL THEN 1 ELSE 0 END")).alias("null_line_id"),
        fsum(expr("CASE WHEN status_severity IS NULL THEN 1 ELSE 0 END")).alias(
            "null_status_severity"
        ),
        fmin(col("event_time")).alias("min_event_time"),
        fmax(col("event_time")).alias("max_event_time"),
    ).collect()[0]

    total_rows = int(agg["total_rows"])
    null_line_id = int(agg["null_line_id"]) if agg["null_line_id"] is not None else 0
    null_status_sev = (
        int(agg["null_status_severity"])
        if agg["null_status_severity"] is not None
        else 0
    )
    bad_rows = int(bad_status.agg(fcount(lit(1)).alias("c")).collect()[0]["c"])
    good_rows = total_rows - bad_rows

    dq_row = spark.createDataFrame(
        [
            (
                int(batch_id),
                "tfl_line_status_events",
                total_rows,
                good_rows,
                bad_rows,
                null_line_id,
                null_status_sev,
                int(parse_fail_cnt),
                agg["min_event_time"],
                agg["max_event_time"],
            )
        ],
        schema="""
        batch_id long,
        dataset string,
        total_rows long,
        good_rows long,
        bad_rows long,
        null_line_id long,
        null_status_severity long,
        parse_fail_rows long,
        min_event_time timestamp,
        max_event_time timestamp
    """,
    ).withColumn("measured_at", measured_at)

    dq_row.writeTo(f"{CATALOG}.{OPS_NS}.dq_metrics").append()


# ----------------------------
# Streaming read from Bronze + foreachBatch write
# ----------------------------
bronze_stream = spark.readStream.table(BRONZE_FQN)

query = (
    bronze_stream.writeStream.foreachBatch(process_batch)
    .option("checkpointLocation", CHECKPOINT)
    .trigger(processingTime="30 seconds")
    .start()
)

query.awaitTermination()
