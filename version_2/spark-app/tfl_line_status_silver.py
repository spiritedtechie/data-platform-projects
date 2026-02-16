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
    count as fcount,
    sum as fsum,
    min as fmin,
    max as fmax,
    xxhash64,
)
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    ArrayType,
    MapType,
)

# ----------------------------
# Config
# ----------------------------
CATALOG = "local"

BRONZE_NS = "bronze"
BRONZE_TABLE = "tfl_line_status"
BRONZE_FQN = f"{CATALOG}.{BRONZE_NS}.{BRONZE_TABLE}"

SILVER_NS = "silver"
OPS_NS = "ops"
QUAR_NS = "quarantine"

CHECKPOINT = "s3a://lake/checkpoints/silver/tfl_line_status"

GOOD_SERVICE_SEVERITY = 10

# ----------------------------
# Spark session (Iceberg extensions configured in spark-submit)
# ----------------------------
spark = SparkSession.builder.appName("tfl-line-status-silver").getOrCreate()

# One stable identifier per running job instance (for concurrency isolation)
RUN_ID = spark.sparkContext.applicationId  # e.g. "app-20260216123456-0001"

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{SILVER_NS}")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{OPS_NS}")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{QUAR_NS}")

# ----------------------------
# Target tables
# ----------------------------

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_NS}.tfl_line_status_events (
  event_id STRING,
  
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,        

  source_created_at_ts TIMESTAMP,

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

  is_disrupted BOOLEAN,

  payload_hash STRING,

  bronze_kafka_topic STRING,
  bronze_kafka_partition INT,
  bronze_kafka_offset BIGINT,
  bronze_kafka_ts TIMESTAMP,
  bronze_kafka_key STRING,

  producer_ingest_ts TIMESTAMP,
  producer_request_id STRING,
  schema_version INT
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{SILVER_NS}.tfl_disruption_events (
  event_id STRING,
  
  event_ts TIMESTAMP,
  ingest_ts TIMESTAMP,

  source_created_at_ts TIMESTAMP,

  line_id STRING,
  disruption_id STRING,

  category STRING,
  category_desc STRING,
  closure_text STRING,
  description STRING,

  payload_hash STRING,

  bronze_kafka_topic STRING,
  bronze_kafka_partition INT,
  bronze_kafka_offset BIGINT,
  bronze_kafka_ts TIMESTAMP,
  bronze_kafka_key STRING,

  producer_ingest_ts TIMESTAMP,
  producer_request_id STRING,
  schema_version INT    
)
USING iceberg
PARTITIONED BY (days(event_ts))
TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{QUAR_NS}.tfl_bad_records (
  ingest_ts TIMESTAMP,
  kafka_topic STRING,
  kafka_partition INT,
  kafka_offset BIGINT,
  kafka_key STRING,
  payload STRING,
  error_reason STRING,
  measured_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')
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

  min_event_ts TIMESTAMP,
  max_event_ts TIMESTAMP,

  measured_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2','write.format.default'='parquet')
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
        StructField("disruption", disruption_schema, True),
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
        StructField("source", StringType(), True),
        StructField("endpoint", StringType(), True),
        StructField("modes", ArrayType(StringType()), True),
        StructField("request_id", StringType(), True),
        StructField("ingested_at", StringType(), True),
        StructField("http_status", IntegerType(), True),
        StructField("duration_ms", LongType(), True),
        StructField("response_headers", MapType(StringType(), StringType()), True),
        StructField("payload_hash", StringType(), True),
        StructField("payload", ArrayType(line_schema), True),
        StructField("schema_version", IntegerType(), True),
    ]
)


# ----------------------------
# Batch transform (optimized): operates ONLY on the micro-batch DF
# ----------------------------
def process_batch(bronze_batch_df: DataFrame, batch_id: int):
    ss = bronze_batch_df.sparkSession
    measured_at = current_timestamp()
    batch_id_lit = lit(int(batch_id)).cast("bigint")

    # Expect bronze columns: topic, partition, offset, kafka_timestamp, key, value, ingested_at
    b = bronze_batch_df.select(
        col("ingest_ts").cast("timestamp").alias("bronze_ingest_ts"),
        col("kafka_topic").alias("bronze_kafka_topic"),
        col("kafka_partition").cast("int").alias("bronze_kafka_partition"),
        col("kafka_offset").cast("bigint").alias("bronze_kafka_offset"),
        col("kafka_ts").cast("timestamp").alias("bronze_kafka_ts"),
        col("kafka_key").cast("string").alias("bronze_kafka_key"),
        col("payload").cast("string").alias("bronze_payload"),
        col("payload_hash").cast("string").alias("bronze_payload_hash"),
    )

    # Parse envelope JSON
    p = b.withColumn("env", from_json(col("bronze_payload"), envelope_schema))

    # Quarantine parse failures
    parse_fail = p.filter(col("env").isNull()).select(
        col("bronze_ingest_ts").alias("ingest_ts"),
        col("bronze_kafka_topic").alias("kafka_topic"),
        col("bronze_kafka_partition").alias("kafka_partition"),
        col("bronze_kafka_offset").alias("kafka_offset"),
        col("bronze_kafka_key").alias("kafka_key"),
        col("bronze_payload").alias("payload"),
        lit("JSON_PARSE_FAILED").alias("error_reason"),
        measured_at.alias("measured_at"),
    )

    good_env = p.filter(col("env").isNotNull())

    # Envelope fields + robust event_time fallback
    env_norm = (
        good_env
        # observed time (independent of validity)
        .withColumn(
            "event_ts", coalesce(col("bronze_kafka_ts"), col("bronze_ingest_ts"))
        )
        .withColumn("producer_ingest_ts", to_timestamp(col("env.ingested_at")))
        .withColumn("producer_request_id", col("env.request_id"))
        .withColumn("schema_version", coalesce(col("env.schema_version"), lit(1)))
        .withColumn(
            "payload_hash",
            coalesce(col("env.payload_hash"), col("bronze_payload_hash")),
        )
    )

    # Explode lines and statuses
    lines = env_norm.withColumn("line", explode_outer(col("env.payload")))

    statuses = (
        lines.withColumn("status", explode_outer(col("line.lineStatuses")))
        .select(
            col("event_ts"),
            col("bronze_ingest_ts").alias("ingest_ts"),
            to_timestamp(col("status.created")).alias("source_created_at_ts"),
            col("line.id").alias("line_id"),
            col("line.name").alias("line_name"),
            col("line.modeName").alias("mode"),
            col("status.id").cast("bigint").alias("status_id"),
            col("status.statusSeverity").alias("status_severity"),
            col("status.statusSeverityDescription").alias("status_desc"),
            col("status.reason").alias("reason"),
            col("status.validityPeriods").alias("validity_periods"),
            col("status.disruption").alias("disruption"),
            col("bronze_kafka_topic"),
            col("bronze_kafka_partition"),
            col("bronze_kafka_offset"),
            col("bronze_kafka_ts"),
            col("bronze_kafka_key"),
            col("producer_ingest_ts"),
            col("producer_request_id"),
            col("schema_version"),
            col("bronze_payload"),
            col("payload_hash"),
        )
        .withColumn(
            "event_id",
            xxhash64(
                col("bronze_kafka_topic"),
                col("bronze_kafka_partition"),
                col("bronze_kafka_offset"),
            ).cast("string"),
        )
    )

    # validityPeriods -> valid_from/valid_to/is_now (domain window, independent)
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
                "CASE "
                "WHEN size(vp_now) > 0 THEN vp_now[0] "
                "WHEN size(validity_periods) > 0 THEN validity_periods[0] "
                "ELSE NULL END"
            ),
        )
        .withColumn("valid_from", to_timestamp(col("vp_pick.fromDate")))
        .withColumn("valid_to", to_timestamp(col("vp_pick.toDate")))
        .withColumn(
            "is_now",
            expr(
                "CASE WHEN vp_pick IS NULL THEN false "
                "ELSE lower(cast(vp_pick.isNow as string)) = 'true' END"
            ),
        )
        .withColumn(
            "is_disrupted",
            expr(
                f"CASE WHEN status_severity IS NULL THEN NULL "
                f"WHEN status_severity = {GOOD_SERVICE_SEVERITY} THEN false "
                f"ELSE true END"
            ),
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
    bad_required = bad_status.select(
        col("ingest_ts"),
        col("bronze_kafka_topic").alias("kafka_topic"),
        col("bronze_kafka_partition").alias("kafka_partition"),
        col("bronze_kafka_offset").alias("kafka_offset"),
        col("bronze_kafka_key").alias("kafka_key"),
        col("bronze_payload").alias("payload"),
        lit("REQUIRED_FIELD_MISSING").alias("error_reason"),
        measured_at.alias("measured_at"),
    ).dropDuplicates(
        [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "error_reason",
        ]
    )

    # Silver: line status events
    status_events_out = good_status.select(
        "event_id",
        "event_ts",
        "ingest_ts",
        "source_created_at_ts",
        "line_id",
        "line_name",
        "mode",
        "status_id",
        "status_severity",
        "status_desc",
        "reason",
        "valid_from",
        "valid_to",
        "is_now",
        "is_disrupted",
        "payload_hash",
        "bronze_kafka_topic",
        "bronze_kafka_partition",
        "bronze_kafka_offset",
        "bronze_kafka_ts",
        "bronze_kafka_key",
        "producer_ingest_ts",
        "producer_request_id",
        "schema_version",
    ).dropDuplicates(
        [
            "bronze_kafka_topic",
            "bronze_kafka_partition",
            "bronze_kafka_offset",
            "line_id",
            "status_id",
            "status_severity",
        ]
    )

    # Silver: disruption events (only where disruption is present)
    disruption_events_out = (
        statuses.filter(col("disruption").isNotNull() & col("line_id").isNotNull())
        .withColumn("category", col("disruption.category"))
        .withColumn("category_desc", col("disruption.categoryDescription"))
        .withColumn("closure_text", col("disruption.closureText"))
        .withColumn("description", col("disruption.description"))
        .withColumn(
            "disruption_id",
            xxhash64(
                col("line_id"),
                coalesce(col("category"), lit("")),
                coalesce(col("closure_text"), lit("")),
                coalesce(col("description"), lit("")),
                col("event_ts").cast("string"),
            ).cast("string"),
        )
        .withColumn(
            "event_id",
            xxhash64(
                col("bronze_kafka_topic"),
                col("bronze_kafka_partition"),
                col("bronze_kafka_offset"),
                col("disruption_id"),
            ).cast("string"),
        )
        .select(
            "event_id",
            "event_ts",
            col("ingest_ts"),
            "source_created_at_ts",
            "line_id",
            "disruption_id",
            "category",
            "category_desc",
            "closure_text",
            "description",
            "payload_hash",
            "bronze_kafka_topic",
            "bronze_kafka_partition",
            "bronze_kafka_offset",
            "bronze_kafka_ts",
            "bronze_kafka_key",
            "producer_ingest_ts",
            "producer_request_id",
            "schema_version",
        )
        .dropDuplicates(["event_id"])
    )

    # ----------------------------
    # Write outputs (append)
    # ----------------------------
    # Quarantine first (so you can debug even if silver fails later)
    parse_fail.writeTo(f"{CATALOG}.{QUAR_NS}.tfl_bad_records").append()
    bad_required.writeTo(f"{CATALOG}.{QUAR_NS}.tfl_bad_records").append()

    # Line status events
    temp_view_name = f"tmp_status_{RUN_ID.replace('-', '_')}_{batch_id}"

    status_events_out.createOrReplaceTempView(temp_view_name)

    ss.sql(f"""
        MERGE INTO {CATALOG}.{SILVER_NS}.tfl_line_status_events t
        USING {temp_view_name} s
        ON  t.bronze_kafka_topic = s.bronze_kafka_topic
        AND t.bronze_kafka_partition = s.bronze_kafka_partition
        AND t.bronze_kafka_offset = s.bronze_kafka_offset
        AND t.line_id = s.line_id
        AND t.status_id <=> s.status_id
        AND t.status_severity <=> s.status_severity
        WHEN NOT MATCHED THEN INSERT *
    """)

    ss.catalog.dropTempView(temp_view_name)

    # Disruption events
    temp_view_name = f"tmp_disruption_{RUN_ID.replace('-', '_')}_{batch_id}"

    disruption_events_out.createOrReplaceTempView(temp_view_name)

    ss.sql(f"""
        MERGE INTO {CATALOG}.{SILVER_NS}.tfl_disruption_events t
        USING {temp_view_name} s
        ON  t.bronze_kafka_topic = s.bronze_kafka_topic
        AND t.bronze_kafka_partition = s.bronze_kafka_partition
        AND t.bronze_kafka_offset = s.bronze_kafka_offset
        AND t.line_id = s.line_id
        AND t.disruption_id = s.disruption_id
        WHEN NOT MATCHED THEN INSERT *
        
    """)

    ss.catalog.dropTempView(temp_view_name)

    # ----------------------------
    # DQ metrics (single-pass aggregations)
    # ----------------------------
    parse_fail_cnt = parse_fail.count()

    dq_base = statuses.agg(
        fcount(lit(1)).alias("total_rows"),
        fsum(expr("CASE WHEN line_id IS NULL THEN 1 ELSE 0 END")).alias("null_line_id"),
        fsum(expr("CASE WHEN status_severity IS NULL THEN 1 ELSE 0 END")).alias(
            "null_status_severity"
        ),
        fsum(
            expr(
                "CASE WHEN line_id IS NULL OR status_severity IS NULL THEN 1 ELSE 0 END"
            )
        ).alias("bad_rows"),
        fmin(col("event_ts")).alias("min_event_ts"),
        fmax(col("event_ts")).alias("max_event_ts"),
    )

    dq_row = dq_base.select(
        lit(int(batch_id)).cast("bigint").alias("batch_id"),
        lit("tfl_line_status_events").alias("dataset"),
        col("total_rows").cast("bigint").alias("total_rows"),
        (col("total_rows") - col("bad_rows")).cast("bigint").alias("good_rows"),
        col("bad_rows").cast("bigint").alias("bad_rows"),
        col("null_line_id").cast("bigint").alias("null_line_id"),
        col("null_status_severity").cast("bigint").alias("null_status_severity"),
        lit(int(parse_fail_cnt)).cast("bigint").alias("parse_fail_rows"),
        col("min_event_ts"),
        col("max_event_ts"),
        measured_at.alias("measured_at"),
    )

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
