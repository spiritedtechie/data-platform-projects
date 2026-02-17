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
from pyspark import StorageLevel

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
JSON_PARSE_FAILED = "JSON_PARSE_FAILED"
REQUIRED_FIELD_MISSING = "REQUIRED_FIELD_MISSING"
ICEBERG_DEFAULT_TBLPROPS = {
    "format-version": "2",
    "write.format.default": "parquet",
}

TABLE_SPECS = {
    "tfl_line_status_events": {
        "namespace": SILVER_NS,
        "columns": [
            ("event_id", "STRING"),
            ("event_ts", "TIMESTAMP"),
            ("ingest_ts", "TIMESTAMP"),
            ("line_id", "STRING"),
            ("line_name", "STRING"),
            ("mode", "STRING"),
            ("status_id", "BIGINT"),
            ("status_severity", "INT"),
            ("status_desc", "STRING"),
            ("reason", "STRING"),
            ("valid_from", "TIMESTAMP"),
            ("valid_to", "TIMESTAMP"),
            ("is_now", "BOOLEAN"),
            ("is_disrupted", "BOOLEAN"),
            ("payload_hash", "STRING"),
            ("bronze_kafka_topic", "STRING"),
            ("bronze_kafka_partition", "INT"),
            ("bronze_kafka_offset", "BIGINT"),
            ("bronze_kafka_ts", "TIMESTAMP"),
            ("bronze_kafka_key", "STRING"),
            ("producer_ingest_ts", "TIMESTAMP"),
            ("producer_request_id", "STRING"),
            ("schema_version", "INT"),
        ],
        "partition_by": "days(event_ts)",
    },
    "tfl_disruption_events": {
        "namespace": SILVER_NS,
        "columns": [
            ("event_id", "STRING"),
            ("event_ts", "TIMESTAMP"),
            ("ingest_ts", "TIMESTAMP"),
            ("line_id", "STRING"),
            ("disruption_id", "STRING"),
            ("category", "STRING"),
            ("category_desc", "STRING"),
            ("closure_text", "STRING"),
            ("description", "STRING"),
            ("payload_hash", "STRING"),
            ("bronze_kafka_topic", "STRING"),
            ("bronze_kafka_partition", "INT"),
            ("bronze_kafka_offset", "BIGINT"),
            ("bronze_kafka_ts", "TIMESTAMP"),
            ("bronze_kafka_key", "STRING"),
            ("producer_ingest_ts", "TIMESTAMP"),
            ("producer_request_id", "STRING"),
            ("schema_version", "INT"),
        ],
        "partition_by": "days(event_ts)",
    },
    "tfl_bad_records": {
        "namespace": QUAR_NS,
        "columns": [
            ("ingest_ts", "TIMESTAMP"),
            ("kafka_topic", "STRING"),
            ("kafka_partition", "INT"),
            ("kafka_offset", "BIGINT"),
            ("kafka_key", "STRING"),
            ("payload", "STRING"),
            ("error_reason", "STRING"),
            ("measured_at", "TIMESTAMP"),
        ],
    },
    "dq_metrics": {
        "namespace": OPS_NS,
        "columns": [
            ("batch_id", "BIGINT"),
            ("dataset", "STRING"),
            ("total_rows", "BIGINT"),
            ("good_rows", "BIGINT"),
            ("bad_rows", "BIGINT"),
            ("null_line_id", "BIGINT"),
            ("null_status_severity", "BIGINT"),
            ("parse_fail_rows", "BIGINT"),
            ("min_event_ts", "TIMESTAMP"),
            ("max_event_ts", "TIMESTAMP"),
            ("measured_at", "TIMESTAMP"),
        ],
    },
}


def columns_from_spec(table_name: str) -> list[str]:
    return [name for name, _ in TABLE_SPECS[table_name]["columns"]]


STATUS_EVENT_COLS = columns_from_spec("tfl_line_status_events")
DISRUPTION_EVENT_COLS = columns_from_spec("tfl_disruption_events")
QUARANTINE_COLS = columns_from_spec("tfl_bad_records")
DQ_METRIC_COLS = columns_from_spec("dq_metrics")


def create_table_sql(catalog: str, table_name: str, spec: dict) -> str:
    cols_sql = ",\n  ".join([f"{name} {dtype}" for name, dtype in spec["columns"]])
    props = spec.get("tblproperties", ICEBERG_DEFAULT_TBLPROPS)
    props_sql = ", ".join([f"'{k}'='{v}'" for k, v in props.items()])
    partition = spec.get("partition_by")
    partition_sql = f"\nPARTITIONED BY ({partition})" if partition else ""
    return (
        f"CREATE TABLE IF NOT EXISTS {catalog}.{spec['namespace']}.{table_name} (\n"
        f"  {cols_sql}\n"
        f")\n"
        f"USING iceberg"
        f"{partition_sql}\n"
        f"TBLPROPERTIES ({props_sql})"
    )

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
for table_name, table_spec in TABLE_SPECS.items():
    spark.sql(create_table_sql(CATALOG, table_name, table_spec))

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
def parse_iso_ts(c):
    # TfL and producer timestamps can arrive in multiple ISO-8601 variants.
    return coalesce(
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"),
        to_timestamp(c, "yyyy-MM-dd'T'HH:mm:ss'Z'"),
        to_timestamp(c),
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


def select_bronze_batch(bronze_batch_df: DataFrame) -> DataFrame:
    return bronze_batch_df.select(
        col("ingest_ts").cast("timestamp").alias("bronze_ingest_ts"),
        col("kafka_topic").alias("bronze_kafka_topic"),
        col("kafka_partition").cast("int").alias("bronze_kafka_partition"),
        col("kafka_offset").cast("bigint").alias("bronze_kafka_offset"),
        col("kafka_ts").cast("timestamp").alias("bronze_kafka_ts"),
        col("kafka_key").cast("string").alias("bronze_kafka_key"),
        col("payload").cast("string").alias("bronze_payload"),
        col("payload_hash").cast("string").alias("bronze_payload_hash"),
    )


def parse_envelopes(bronze_selected_df: DataFrame, measured_at) -> tuple[DataFrame, DataFrame]:
    parsed = bronze_selected_df.withColumn(
        "env", from_json(col("bronze_payload"), envelope_schema)
    ).persist(StorageLevel.MEMORY_AND_DISK)
    parse_fail = parsed.filter(col("env").isNull()).select(
        col("bronze_ingest_ts").alias("ingest_ts"),
        col("bronze_kafka_topic").alias("kafka_topic"),
        col("bronze_kafka_partition").alias("kafka_partition"),
        col("bronze_kafka_offset").alias("kafka_offset"),
        col("bronze_kafka_key").alias("kafka_key"),
        col("bronze_payload").alias("payload"),
        lit(JSON_PARSE_FAILED).alias("error_reason"),
        measured_at.alias("measured_at"),
    )
    return parsed, parse_fail


def build_statuses(good_env: DataFrame) -> DataFrame:
    env_norm = (
        good_env.withColumn(
            "event_ts", coalesce(col("bronze_kafka_ts"), col("bronze_ingest_ts"))
        )
        .withColumn("producer_ingest_ts", parse_iso_ts(col("env.ingested_at")))
        .withColumn("producer_request_id", col("env.request_id"))
        .withColumn("schema_version", coalesce(col("env.schema_version"), lit(1)))
        .withColumn(
            "payload_hash",
            coalesce(col("env.payload_hash"), col("bronze_payload_hash")),
        )
    )
    lines = env_norm.withColumn("line", explode_outer(col("env.payload")))
    statuses = (
        lines.withColumn("status", explode_outer(col("line.lineStatuses")))
        .select(
            col("event_ts"),
            col("bronze_ingest_ts").alias("ingest_ts"),
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
                col("line_id"),
                col("status_id"),
                col("status_severity"),
            ).cast("string"),
        )
    )
    return (
        statuses.withColumn(
            "vp_now",
            expr("filter(validity_periods, x -> lower(cast(x.isNow as string)) = 'true')"),
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
        .withColumn("valid_from", parse_iso_ts(col("vp_pick.fromDate")))
        .withColumn("valid_to", parse_iso_ts(col("vp_pick.toDate")))
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
    ).persist(StorageLevel.MEMORY_AND_DISK)


def build_bad_required(bad_status: DataFrame, measured_at) -> DataFrame:
    return bad_status.select(
        col("ingest_ts"),
        col("bronze_kafka_topic").alias("kafka_topic"),
        col("bronze_kafka_partition").alias("kafka_partition"),
        col("bronze_kafka_offset").alias("kafka_offset"),
        col("bronze_kafka_key").alias("kafka_key"),
        col("bronze_payload").alias("payload"),
        lit(REQUIRED_FIELD_MISSING).alias("error_reason"),
        measured_at.alias("measured_at"),
    ).dropDuplicates(
        [
            "kafka_topic",
            "kafka_partition",
            "kafka_offset",
            "error_reason",
        ]
    )


def build_status_events(good_status: DataFrame) -> DataFrame:
    return good_status.select(*STATUS_EVENT_COLS).dropDuplicates(
        [
            "bronze_kafka_topic",
            "bronze_kafka_partition",
            "bronze_kafka_offset",
            "line_id",
            "status_id",
            "status_severity",
        ]
    )


def build_disruption_events(statuses: DataFrame) -> DataFrame:
    return (
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
        .select(*DISRUPTION_EVENT_COLS)
        .dropDuplicates(["event_id"])
    )


def write_quarantine(
    ss: SparkSession,
    parse_fail: DataFrame,
    bad_required: DataFrame,
    batch_id: int,
):
    quarantine_out = parse_fail.unionByName(bad_required).dropDuplicates(
        ["kafka_topic", "kafka_partition", "kafka_offset", "error_reason"]
    )
    merge_into_table(
        ss=ss,
        df=quarantine_out.select(*QUARANTINE_COLS),
        table_fqn=f"{CATALOG}.{QUAR_NS}.tfl_bad_records",
        on_clause=(
            "t.kafka_topic = s.kafka_topic "
            "AND t.kafka_partition = s.kafka_partition "
            "AND t.kafka_offset = s.kafka_offset "
            "AND t.error_reason = s.error_reason"
        ),
        insert_cols=QUARANTINE_COLS,
        temp_prefix="tmp_quarantine",
        run_id=RUN_ID,
        batch_id=batch_id,
    )


def write_silver_events(
    ss: SparkSession,
    status_events_out: DataFrame,
    disruption_events_out: DataFrame,
    batch_id: int,
):
    merge_into_table(
        ss=ss,
        df=status_events_out.select(*STATUS_EVENT_COLS),
        table_fqn=f"{CATALOG}.{SILVER_NS}.tfl_line_status_events",
        on_clause=(
            "t.bronze_kafka_topic = s.bronze_kafka_topic "
            "AND t.bronze_kafka_partition = s.bronze_kafka_partition "
            "AND t.bronze_kafka_offset = s.bronze_kafka_offset "
            "AND t.line_id = s.line_id "
            "AND t.status_id <=> s.status_id "
            "AND t.status_severity <=> s.status_severity"
        ),
        insert_cols=STATUS_EVENT_COLS,
        temp_prefix="tmp_status",
        run_id=RUN_ID,
        batch_id=batch_id,
    )
    merge_into_table(
        ss=ss,
        df=disruption_events_out.select(*DISRUPTION_EVENT_COLS),
        table_fqn=f"{CATALOG}.{SILVER_NS}.tfl_disruption_events",
        on_clause=(
            "t.bronze_kafka_topic = s.bronze_kafka_topic "
            "AND t.bronze_kafka_partition = s.bronze_kafka_partition "
            "AND t.bronze_kafka_offset = s.bronze_kafka_offset "
            "AND t.line_id = s.line_id "
            "AND t.disruption_id = s.disruption_id"
        ),
        insert_cols=DISRUPTION_EVENT_COLS,
        temp_prefix="tmp_disruption",
        run_id=RUN_ID,
        batch_id=batch_id,
    )


def write_dq_metrics(
    ss: SparkSession,
    statuses: DataFrame,
    parse_fail: DataFrame,
    measured_at,
    batch_id: int,
):
    parse_fail_cnt = parse_fail.count()
    dq_base = statuses.agg(
        fcount(lit(1)).alias("total_rows"),
        fsum(expr("CASE WHEN line_id IS NULL THEN 1 ELSE 0 END")).alias("null_line_id"),
        fsum(expr("CASE WHEN status_severity IS NULL THEN 1 ELSE 0 END")).alias(
            "null_status_severity"
        ),
        fsum(
            expr("CASE WHEN line_id IS NULL OR status_severity IS NULL THEN 1 ELSE 0 END")
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
    merge_into_table(
        ss=ss,
        df=dq_row.select(*DQ_METRIC_COLS),
        table_fqn=f"{CATALOG}.{OPS_NS}.dq_metrics",
        on_clause="t.batch_id = s.batch_id AND t.dataset = s.dataset",
        insert_cols=DQ_METRIC_COLS,
        temp_prefix="tmp_dq",
        run_id=RUN_ID,
        batch_id=batch_id,
        update_cols=[
            "total_rows",
            "good_rows",
            "bad_rows",
            "null_line_id",
            "null_status_severity",
            "parse_fail_rows",
            "min_event_ts",
            "max_event_ts",
            "measured_at",
        ],
    )


def process_batch(bronze_batch_df: DataFrame, batch_id: int):
    ss = bronze_batch_df.sparkSession
    measured_at = current_timestamp()
    if bronze_batch_df.rdd.isEmpty():
        return

    bronze_selected = select_bronze_batch(bronze_batch_df)
    parsed, parse_fail = parse_envelopes(bronze_selected, measured_at)
    statuses = build_statuses(parsed.filter(col("env").isNotNull()))
    try:
        good_status = statuses.filter(
            col("line_id").isNotNull() & col("status_severity").isNotNull()
        )
        bad_status = statuses.filter(
            col("line_id").isNull() | col("status_severity").isNull()
        )
        bad_required = build_bad_required(bad_status, measured_at)
        status_events_out = build_status_events(good_status)
        disruption_events_out = build_disruption_events(statuses)
        write_quarantine(ss, parse_fail, bad_required, batch_id)
        write_silver_events(ss, status_events_out, disruption_events_out, batch_id)
        write_dq_metrics(ss, statuses, parse_fail, measured_at, batch_id)
    finally:
        statuses.unpersist()
        parsed.unpersist()


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
