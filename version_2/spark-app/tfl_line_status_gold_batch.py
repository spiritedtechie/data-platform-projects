from pyspark.sql import SparkSession, DataFrame

CATALOG = "local"
SILVER_NS = "silver"
GOLD_NS = "gold"
OPS_NS = "ops"
SILVER_EVENTS = f"{CATALOG}.{SILVER_NS}.tfl_line_status_events"
STATE_TABLE = f"{CATALOG}.{OPS_NS}.pipeline_state"
PIPELINE_NAME = "tfl_line_status_gold_batch"

spark = SparkSession.builder.appName("tfl-line-status-gold-batch").getOrCreate()
RUN_ID = spark.sparkContext.applicationId

spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{GOLD_NS}")
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{OPS_NS}")


def merge_into_table(
    ss: SparkSession,
    df: DataFrame,
    table_fqn: str,
    on_clause: str,
    insert_cols: list[str],
    temp_prefix: str,
    run_id: str,
    batch_id: str,
    update_cols: list[str] | None = None,
):
    temp_view = f"{temp_prefix}_{run_id.replace('-', '_')}_{batch_id}"
    insert_sql = ", ".join(insert_cols)
    values_sql = ", ".join([f"s.{c}" for c in insert_cols])
    update_sql = ""
    if update_cols:
        assignments = ",\n              ".join([f"{c} = s.{c}" for c in update_cols])
        update_sql = (
            f"\n            WHEN MATCHED THEN UPDATE SET\n              {assignments}"
        )

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


# ----------------------------
# Table bootstrap
# ----------------------------
spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {STATE_TABLE} (
  pipeline_name STRING,
  last_ingest_ts TIMESTAMP,
  updated_at TIMESTAMP
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.dim_line (
  line_id STRING,
  line_name STRING,
  mode STRING,
  first_seen_ts TIMESTAMP,
  last_seen_ts TIMESTAMP
)
USING iceberg
PARTITIONED BY (mode)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.dim_status (
  status_severity INT,
  status_desc STRING,
  status_category STRING,
  severity_weight DOUBLE,
  is_good_service BOOLEAN
)
USING iceberg
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.dim_datetime (
  date_key DATE,
  hour_of_day INT,
  day_of_week INT,
  day_name STRING,
  week_of_year INT,
  month_num INT,
  month_name STRING,
  quarter_num INT,
  year_num INT,
  is_weekend BOOLEAN
)
USING iceberg
PARTITIONED BY (year_num, month_num)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.fact_line_status_interval (
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
  event_count_in_interval INT
)
USING iceberg
PARTITIONED BY (days(valid_from), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.fact_line_status_change (
  line_id STRING,
  mode STRING,
  change_ts TIMESTAMP,
  prev_change_ts TIMESTAMP,
  time_since_prev_seconds BIGINT,
  prev_status_severity INT,
  prev_status_desc STRING,
  new_status_severity INT,
  new_status_desc STRING,
  recovery_flag BOOLEAN,
  time_in_new_state_seconds BIGINT
)
USING iceberg
PARTITIONED BY (days(change_ts))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.mart_line_hour (
  bucket_hour TIMESTAMP,
  service_date DATE,
  line_id STRING,
  line_name STRING,
  mode STRING,
  total_seconds BIGINT,
  good_service_seconds BIGINT,
  disruption_seconds BIGINT,
  severity_weighted_seconds DOUBLE,
  network_impact_score DOUBLE,
  incident_count BIGINT,
  state_change_count BIGINT,
  good_service_pct DOUBLE
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.mart_line_day (
  service_date DATE,
  line_id STRING,
  line_name STRING,
  mode STRING,
  total_seconds BIGINT,
  good_service_seconds BIGINT,
  disruption_seconds BIGINT,
  severity_weighted_seconds DOUBLE,
  incident_count BIGINT,
  mean_incident_duration_seconds DOUBLE,
  p50_incident_duration_seconds DOUBLE,
  p95_incident_duration_seconds DOUBLE,
  max_incident_duration_seconds BIGINT,
  num_state_changes BIGINT,
  avg_time_in_state_seconds DOUBLE,
  time_to_recover_avg_seconds DOUBLE,
  worst_status_severity INT,
  good_service_pct DOUBLE
)
USING iceberg
PARTITIONED BY (months(service_date), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.mart_network_hour (
  bucket_hour TIMESTAMP,
  service_date DATE,
  lines_reporting BIGINT,
  lines_impacted BIGINT,
  total_seconds BIGINT,
  good_service_seconds BIGINT,
  disruption_seconds BIGINT,
  severity_weighted_seconds DOUBLE,
  network_good_service_pct DOUBLE
)
USING iceberg
PARTITIONED BY (days(bucket_hour))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.mart_network_day (
  service_date DATE,
  lines_reporting BIGINT,
  lines_impacted BIGINT,
  total_seconds BIGINT,
  good_service_seconds BIGINT,
  disruption_seconds BIGINT,
  severity_weighted_seconds DOUBLE,
  network_good_service_pct DOUBLE
)
USING iceberg
PARTITIONED BY (months(service_date))
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.current_line_status (
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
  last_changed_minutes_ago DOUBLE
)
USING iceberg
PARTITIONED BY (mode)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_features_line_hour (
  bucket_hour TIMESTAMP,
  line_id STRING,
  mode STRING,
  hour_of_day INT,
  day_of_week INT,
  disruption_seconds BIGINT,
  good_service_pct DOUBLE,
  state_change_count BIGINT,
  severity_weighted_seconds DOUBLE,
  disruption_ratio DOUBLE,
  lag_1h_disruption_seconds BIGINT,
  lag_2h_disruption_seconds BIGINT,
  lag_24h_disruption_seconds BIGINT,
  rolling_6h_disruption_seconds DOUBLE,
  rolling_24h_disruption_seconds DOUBLE,
  disruption_next_1h_label INT
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_features_line_day (
  service_date DATE,
  line_id STRING,
  mode STRING,
  day_of_week INT,
  downtime_minutes DOUBLE,
  good_service_pct DOUBLE,
  num_state_changes BIGINT,
  severity_weighted_seconds DOUBLE,
  lag_1d_downtime_minutes DOUBLE,
  lag_7d_downtime_minutes DOUBLE,
  rolling_7d_downtime_minutes DOUBLE,
  downtime_next_day_minutes_label DOUBLE
)
USING iceberg
PARTITIONED BY (months(service_date), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_features_incident_start (
  line_id STRING,
  mode STRING,
  incident_start_ts TIMESTAMP,
  status_severity INT,
  hour_of_day INT,
  day_of_week INT,
  recent_24h_incident_count BIGINT,
  recent_24h_mean_incident_seconds DOUBLE,
  recent_24h_state_changes BIGINT,
  incident_duration_seconds_label BIGINT
)
USING iceberg
PARTITIONED BY (days(incident_start_ts), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

spark.sql(
    f"""
CREATE TABLE IF NOT EXISTS {CATALOG}.{GOLD_NS}.ml_anomalies_line_hour (
  bucket_hour TIMESTAMP,
  line_id STRING,
  mode STRING,
  disruption_seconds BIGINT,
  expected_disruption_seconds DOUBLE,
  z_score DOUBLE,
  is_anomaly BOOLEAN,
  anomaly_type STRING
)
USING iceberg
PARTITIONED BY (days(bucket_hour), line_id)
TBLPROPERTIES ('format-version'='2', 'write.format.default'='parquet')
"""
)

# ----------------------------
# Incremental Silver input
# ----------------------------
spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW silver_events AS
SELECT
  event_id,
  event_ts,
  ingest_ts,
  line_id,
  line_name,
  mode,
  status_severity,
  status_desc,
  reason,
  valid_from,
  valid_to,
  is_disrupted,
  payload_hash,
  producer_ingest_ts,
  producer_request_id,
  schema_version
FROM {SILVER_EVENTS}
WHERE line_id IS NOT NULL
"""
)

spark.sql(
    f"""
CREATE OR REPLACE TEMP VIEW watermark AS
SELECT max(last_ingest_ts) AS last_ingest_ts
FROM {STATE_TABLE}
WHERE pipeline_name = '{PIPELINE_NAME}'
"""
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW delta_events AS
SELECT e.*
FROM silver_events e
CROSS JOIN watermark w
WHERE w.last_ingest_ts IS NULL OR e.ingest_ts > w.last_ingest_ts
"""
)

if spark.table("delta_events").limit(1).count() == 0:
    spark.stop()
    raise SystemExit(0)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_lines AS
SELECT DISTINCT line_id
FROM delta_events
"""
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_events AS
SELECT e.*
FROM silver_events e
INNER JOIN affected_lines a
  ON e.line_id = a.line_id
"""
)

# ----------------------------
# Incremental dimensions
# ----------------------------
dim_line_df = spark.sql(
    """
SELECT
  line_id,
  max_by(line_name, event_ts) AS line_name,
  max_by(mode, event_ts) AS mode,
  min(event_ts) AS first_seen_ts,
  max(event_ts) AS last_seen_ts
FROM affected_events
GROUP BY line_id
"""
)

merge_into_table(
    ss=spark,
    df=dim_line_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.dim_line",
    on_clause="t.line_id = s.line_id",
    insert_cols=["line_id", "line_name", "mode", "first_seen_ts", "last_seen_ts"],
    temp_prefix="tmp_dim_line",
    run_id=RUN_ID,
    batch_id="dim_line",
    update_cols=["line_name", "mode", "first_seen_ts", "last_seen_ts"],
)

dim_status_df = spark.sql(
    """
WITH observed AS (
  SELECT DISTINCT status_severity, coalesce(status_desc, 'Unknown') AS status_desc
  FROM affected_events
)
SELECT
  status_severity,
  status_desc,
  CASE
    WHEN status_severity >= 10 THEN 'Good Service'
    WHEN status_severity = 9 THEN 'Minor Delays'
    WHEN status_severity = 8 THEN 'Severe Delays'
    WHEN status_severity IN (7, 6, 5) THEN 'Suspended / Closed'
    ELSE 'Other'
  END AS status_category,
  CASE
    WHEN status_severity >= 10 THEN 0.0
    WHEN status_severity = 9 THEN 1.0
    WHEN status_severity = 8 THEN 2.0
    WHEN status_severity = 7 THEN 3.0
    WHEN status_severity IN (6, 5) THEN 4.0
    ELSE 2.0
  END AS severity_weight,
  CASE WHEN status_severity >= 10 THEN true ELSE false END AS is_good_service
FROM observed
"""
)

merge_into_table(
    ss=spark,
    df=dim_status_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.dim_status",
    on_clause="t.status_severity = s.status_severity",
    insert_cols=[
        "status_severity",
        "status_desc",
        "status_category",
        "severity_weight",
        "is_good_service",
    ],
    temp_prefix="tmp_dim_status",
    run_id=RUN_ID,
    batch_id="dim_status",
    update_cols=["status_desc", "status_category", "severity_weight", "is_good_service"],
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW impacted_dates AS
WITH b AS (
  SELECT
    to_date(min(coalesce(valid_from, event_ts))) AS min_date,
    to_date(max(coalesce(valid_to, event_ts, current_timestamp()))) AS max_date
  FROM delta_events
)
SELECT explode(sequence(min_date, date_add(max_date, 30), interval 1 day)) AS date_key
FROM b
"""
)

dim_datetime_df = spark.sql(
    """
WITH hours AS (
  SELECT explode(sequence(0, 23)) AS hour_of_day
)
SELECT
  d.date_key,
  h.hour_of_day,
  dayofweek(d.date_key) AS day_of_week,
  date_format(d.date_key, 'EEEE') AS day_name,
  weekofyear(d.date_key) AS week_of_year,
  month(d.date_key) AS month_num,
  date_format(d.date_key, 'MMMM') AS month_name,
  quarter(d.date_key) AS quarter_num,
  year(d.date_key) AS year_num,
  CASE WHEN dayofweek(d.date_key) IN (1, 7) THEN true ELSE false END AS is_weekend
FROM impacted_dates d
CROSS JOIN hours h
"""
)

merge_into_table(
    ss=spark,
    df=dim_datetime_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.dim_datetime",
    on_clause="t.date_key = s.date_key AND t.hour_of_day = s.hour_of_day",
    insert_cols=[
        "date_key",
        "hour_of_day",
        "day_of_week",
        "day_name",
        "week_of_year",
        "month_num",
        "month_name",
        "quarter_num",
        "year_num",
        "is_weekend",
    ],
    temp_prefix="tmp_dim_datetime",
    run_id=RUN_ID,
    batch_id="dim_datetime",
    update_cols=[
        "day_of_week",
        "day_name",
        "week_of_year",
        "month_num",
        "month_name",
        "quarter_num",
        "year_num",
        "is_weekend",
    ],
)

# ----------------------------
# Recompute intervals for affected lines only
# ----------------------------
spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_intervals AS
WITH base AS (
  SELECT
    line_id,
    line_name,
    mode,
    event_id,
    event_ts,
    ingest_ts,
    status_severity,
    coalesce(status_desc, 'Unknown') AS status_desc,
    reason,
    coalesce(valid_from, event_ts) AS interval_start,
    valid_to AS interval_end_raw,
    coalesce(is_disrupted, CASE WHEN status_severity >= 10 THEN false ELSE true END) AS is_disrupted,
    sha2(coalesce(reason, ''), 256) AS reason_text_hash
  FROM affected_events
), sequenced AS (
  SELECT
    *,
    lead(interval_start) OVER (PARTITION BY line_id ORDER BY interval_start, event_ts, ingest_ts) AS next_interval_start
  FROM base
), bounded AS (
  SELECT
    line_id,
    line_name,
    mode,
    event_id,
    event_ts,
    ingest_ts,
    status_severity,
    status_desc,
    reason,
    reason_text_hash,
    interval_start AS valid_from,
    CASE
      WHEN coalesce(interval_end_raw, next_interval_start, current_timestamp()) <= interval_start
        THEN interval_start + INTERVAL 1 SECOND
      ELSE coalesce(interval_end_raw, next_interval_start, current_timestamp())
    END AS valid_to,
    is_disrupted
  FROM sequenced
)
SELECT
  line_id,
  line_name,
  mode,
  event_id,
  event_ts,
  ingest_ts,
  valid_from,
  valid_to,
  cast(unix_timestamp(valid_to) - unix_timestamp(valid_from) AS BIGINT) AS interval_seconds,
  status_severity,
  status_desc,
  CASE WHEN status_severity >= 10 THEN true ELSE false END AS is_good_service,
  is_disrupted,
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
  END AS disruption_category,
  reason,
  reason_text_hash,
  1 AS event_count_in_interval
FROM bounded
"""
)

merge_into_table(
    ss=spark,
    df=spark.table("affected_intervals"),
    table_fqn=f"{CATALOG}.{GOLD_NS}.fact_line_status_interval",
    on_clause=(
        "t.line_id = s.line_id "
        "AND t.event_id = s.event_id "
        "AND t.status_severity <=> s.status_severity"
    ),
    insert_cols=[
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
    ],
    temp_prefix="tmp_fact_interval",
    run_id=RUN_ID,
    batch_id="fact_interval",
    update_cols=[
        "line_name",
        "mode",
        "event_ts",
        "ingest_ts",
        "valid_from",
        "valid_to",
        "interval_seconds",
        "status_desc",
        "is_good_service",
        "is_disrupted",
        "disruption_category",
        "reason",
        "reason_text_hash",
        "event_count_in_interval",
    ],
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_changes AS
WITH ordered AS (
  SELECT
    line_id,
    mode,
    valid_from AS change_ts,
    valid_to,
    interval_seconds,
    status_severity AS new_status_severity,
    status_desc AS new_status_desc,
    lag(status_severity) OVER (PARTITION BY line_id ORDER BY valid_from, event_ts) AS prev_status_severity,
    lag(status_desc) OVER (PARTITION BY line_id ORDER BY valid_from, event_ts) AS prev_status_desc,
    lag(valid_from) OVER (PARTITION BY line_id ORDER BY valid_from, event_ts) AS prev_change_ts
  FROM affected_intervals
)
SELECT
  line_id,
  mode,
  change_ts,
  prev_change_ts,
  cast(
    CASE
      WHEN prev_change_ts IS NULL THEN NULL
      ELSE unix_timestamp(change_ts) - unix_timestamp(prev_change_ts)
    END AS BIGINT
  ) AS time_since_prev_seconds,
  prev_status_severity,
  prev_status_desc,
  new_status_severity,
  new_status_desc,
  CASE
    WHEN prev_status_severity IS NOT NULL
      AND prev_status_severity < 10
      AND new_status_severity >= 10 THEN true
    ELSE false
  END AS recovery_flag,
  interval_seconds AS time_in_new_state_seconds
FROM ordered
WHERE prev_status_severity IS NOT NULL
"""
)

merge_into_table(
    ss=spark,
    df=spark.table("affected_changes"),
    table_fqn=f"{CATALOG}.{GOLD_NS}.fact_line_status_change",
    on_clause=(
        "t.line_id = s.line_id "
        "AND t.change_ts = s.change_ts "
        "AND t.new_status_severity <=> s.new_status_severity"
    ),
    insert_cols=[
        "line_id",
        "mode",
        "change_ts",
        "prev_change_ts",
        "time_since_prev_seconds",
        "prev_status_severity",
        "prev_status_desc",
        "new_status_severity",
        "new_status_desc",
        "recovery_flag",
        "time_in_new_state_seconds",
    ],
    temp_prefix="tmp_fact_change",
    run_id=RUN_ID,
    batch_id="fact_change",
    update_cols=[
        "mode",
        "prev_change_ts",
        "time_since_prev_seconds",
        "prev_status_severity",
        "prev_status_desc",
        "new_status_desc",
        "recovery_flag",
        "time_in_new_state_seconds",
    ],
)

# ----------------------------
# Affected bucket scaffolding
# ----------------------------
spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_hours AS
SELECT DISTINCT date_trunc('hour', valid_from) AS bucket_hour
FROM delta_events
UNION
SELECT DISTINCT date_trunc('hour', coalesce(valid_to, current_timestamp())) AS bucket_hour
FROM delta_events
"""
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_days AS
SELECT DISTINCT to_date(bucket_hour) AS service_date
FROM affected_hours
"""
)

# ----------------------------
# Line marts (incremental upsert)
# ----------------------------
spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_interval_hour_overlap AS
WITH expanded AS (
  SELECT
    i.*,
    explode(
      sequence(
        date_trunc('hour', i.valid_from),
        date_trunc('hour', i.valid_to),
        interval 1 hour
      )
    ) AS bucket_hour
  FROM affected_intervals i
), bounded AS (
  SELECT
    line_id,
    line_name,
    mode,
    status_severity,
    is_disrupted,
    is_good_service,
    bucket_hour,
    greatest(valid_from, bucket_hour) AS overlap_start,
    least(valid_to, bucket_hour + interval 1 hour) AS overlap_end
  FROM expanded
)
SELECT
  line_id,
  line_name,
  mode,
  status_severity,
  is_disrupted,
  is_good_service,
  bucket_hour,
  cast(unix_timestamp(overlap_end) - unix_timestamp(overlap_start) AS BIGINT) AS overlap_seconds
FROM bounded
WHERE overlap_end > overlap_start
"""
)

line_hour_df = spark.sql(
    f"""
WITH change_hour AS (
  SELECT
    line_id,
    date_trunc('hour', change_ts) AS bucket_hour,
    count(*) AS state_change_count
  FROM affected_changes
  GROUP BY line_id, date_trunc('hour', change_ts)
)
SELECT
  h.bucket_hour,
  to_date(h.bucket_hour) AS service_date,
  h.line_id,
  max(h.line_name) AS line_name,
  max(h.mode) AS mode,
  sum(h.overlap_seconds) AS total_seconds,
  sum(CASE WHEN h.is_good_service THEN h.overlap_seconds ELSE 0 END) AS good_service_seconds,
  sum(CASE WHEN h.is_disrupted THEN h.overlap_seconds ELSE 0 END) AS disruption_seconds,
  sum(h.overlap_seconds * coalesce(s.severity_weight, 0.0)) AS severity_weighted_seconds,
  sum(h.overlap_seconds * coalesce(s.severity_weight, 0.0)) / 3600.0 AS network_impact_score,
  count_if(h.is_disrupted) AS incident_count,
  coalesce(max(c.state_change_count), 0) AS state_change_count,
  CASE WHEN sum(h.overlap_seconds) > 0
       THEN sum(CASE WHEN h.is_good_service THEN h.overlap_seconds ELSE 0 END) / sum(h.overlap_seconds)
       ELSE NULL END AS good_service_pct
FROM affected_interval_hour_overlap h
LEFT JOIN {CATALOG}.{GOLD_NS}.dim_status s
  ON h.status_severity = s.status_severity
LEFT JOIN change_hour c
  ON h.line_id = c.line_id AND h.bucket_hour = c.bucket_hour
GROUP BY h.bucket_hour, h.line_id
"""
)

merge_into_table(
    ss=spark,
    df=line_hour_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.mart_line_hour",
    on_clause="t.bucket_hour = s.bucket_hour AND t.line_id = s.line_id",
    insert_cols=[
        "bucket_hour",
        "service_date",
        "line_id",
        "line_name",
        "mode",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_impact_score",
        "incident_count",
        "state_change_count",
        "good_service_pct",
    ],
    temp_prefix="tmp_mart_line_hour",
    run_id=RUN_ID,
    batch_id="mart_line_hour",
    update_cols=[
        "service_date",
        "line_name",
        "mode",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_impact_score",
        "incident_count",
        "state_change_count",
        "good_service_pct",
    ],
)

spark.sql(
    """
CREATE OR REPLACE TEMP VIEW affected_interval_day_overlap AS
WITH expanded AS (
  SELECT
    i.*,
    explode(
      sequence(
        date_trunc('day', i.valid_from),
        date_trunc('day', i.valid_to),
        interval 1 day
      )
    ) AS bucket_day
  FROM affected_intervals i
), bounded AS (
  SELECT
    line_id,
    line_name,
    mode,
    status_severity,
    is_disrupted,
    is_good_service,
    to_date(bucket_day) AS service_date,
    greatest(valid_from, bucket_day) AS overlap_start,
    least(valid_to, bucket_day + interval 1 day) AS overlap_end
  FROM expanded
)
SELECT
  line_id,
  line_name,
  mode,
  status_severity,
  is_disrupted,
  is_good_service,
  service_date,
  cast(unix_timestamp(overlap_end) - unix_timestamp(overlap_start) AS BIGINT) AS overlap_seconds
FROM bounded
WHERE overlap_end > overlap_start
"""
)

line_day_df = spark.sql(
    f"""
WITH incident_stats AS (
  SELECT
    line_id,
    to_date(valid_from) AS service_date,
    avg(interval_seconds) AS mean_incident_duration_seconds,
    percentile_approx(interval_seconds, 0.5) AS p50_incident_duration_seconds,
    percentile_approx(interval_seconds, 0.95) AS p95_incident_duration_seconds,
    max(interval_seconds) AS max_incident_duration_seconds
  FROM affected_intervals
  WHERE is_disrupted = true
  GROUP BY line_id, to_date(valid_from)
), change_stats AS (
  SELECT
    line_id,
    to_date(change_ts) AS service_date,
    count(*) AS num_state_changes,
    avg(time_in_new_state_seconds) AS avg_time_in_state_seconds,
    avg(CASE WHEN recovery_flag THEN time_since_prev_seconds END) AS time_to_recover_avg_seconds
  FROM affected_changes
  GROUP BY line_id, to_date(change_ts)
)
SELECT
  d.service_date,
  d.line_id,
  max(d.line_name) AS line_name,
  max(d.mode) AS mode,
  sum(d.overlap_seconds) AS total_seconds,
  sum(CASE WHEN d.is_good_service THEN d.overlap_seconds ELSE 0 END) AS good_service_seconds,
  sum(CASE WHEN d.is_disrupted THEN d.overlap_seconds ELSE 0 END) AS disruption_seconds,
  sum(d.overlap_seconds * coalesce(s.severity_weight, 0.0)) AS severity_weighted_seconds,
  count_if(d.is_disrupted) AS incident_count,
  i.mean_incident_duration_seconds,
  i.p50_incident_duration_seconds,
  i.p95_incident_duration_seconds,
  i.max_incident_duration_seconds,
  coalesce(c.num_state_changes, 0) AS num_state_changes,
  c.avg_time_in_state_seconds,
  c.time_to_recover_avg_seconds,
  min(d.status_severity) AS worst_status_severity,
  CASE WHEN sum(d.overlap_seconds) > 0
       THEN sum(CASE WHEN d.is_good_service THEN d.overlap_seconds ELSE 0 END) / sum(d.overlap_seconds)
       ELSE NULL END AS good_service_pct
FROM affected_interval_day_overlap d
LEFT JOIN {CATALOG}.{GOLD_NS}.dim_status s
  ON d.status_severity = s.status_severity
LEFT JOIN incident_stats i
  ON d.line_id = i.line_id AND d.service_date = i.service_date
LEFT JOIN change_stats c
  ON d.line_id = c.line_id AND d.service_date = c.service_date
GROUP BY d.service_date, d.line_id, i.mean_incident_duration_seconds, i.p50_incident_duration_seconds,
  i.p95_incident_duration_seconds, i.max_incident_duration_seconds, c.num_state_changes,
  c.avg_time_in_state_seconds, c.time_to_recover_avg_seconds
"""
)

merge_into_table(
    ss=spark,
    df=line_day_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.mart_line_day",
    on_clause="t.service_date = s.service_date AND t.line_id = s.line_id",
    insert_cols=[
        "service_date",
        "line_id",
        "line_name",
        "mode",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "incident_count",
        "mean_incident_duration_seconds",
        "p50_incident_duration_seconds",
        "p95_incident_duration_seconds",
        "max_incident_duration_seconds",
        "num_state_changes",
        "avg_time_in_state_seconds",
        "time_to_recover_avg_seconds",
        "worst_status_severity",
        "good_service_pct",
    ],
    temp_prefix="tmp_mart_line_day",
    run_id=RUN_ID,
    batch_id="mart_line_day",
    update_cols=[
        "line_name",
        "mode",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "incident_count",
        "mean_incident_duration_seconds",
        "p50_incident_duration_seconds",
        "p95_incident_duration_seconds",
        "max_incident_duration_seconds",
        "num_state_changes",
        "avg_time_in_state_seconds",
        "time_to_recover_avg_seconds",
        "worst_status_severity",
        "good_service_pct",
    ],
)

# ----------------------------
# Network marts recomputed only for impacted buckets
# ----------------------------
network_hour_df = spark.sql(
    f"""
SELECT
  h.bucket_hour,
  to_date(h.bucket_hour) AS service_date,
  count(distinct h.line_id) AS lines_reporting,
  count(distinct CASE WHEN h.disruption_seconds > 0 THEN h.line_id END) AS lines_impacted,
  sum(h.total_seconds) AS total_seconds,
  sum(h.good_service_seconds) AS good_service_seconds,
  sum(h.disruption_seconds) AS disruption_seconds,
  sum(h.severity_weighted_seconds) AS severity_weighted_seconds,
  CASE WHEN sum(h.total_seconds) > 0
    THEN sum(h.good_service_seconds) / sum(h.total_seconds)
    ELSE NULL END AS network_good_service_pct
FROM {CATALOG}.{GOLD_NS}.mart_line_hour h
INNER JOIN affected_hours a
  ON h.bucket_hour = a.bucket_hour
GROUP BY h.bucket_hour
"""
)

merge_into_table(
    ss=spark,
    df=network_hour_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.mart_network_hour",
    on_clause="t.bucket_hour = s.bucket_hour",
    insert_cols=[
        "bucket_hour",
        "service_date",
        "lines_reporting",
        "lines_impacted",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_good_service_pct",
    ],
    temp_prefix="tmp_mart_network_hour",
    run_id=RUN_ID,
    batch_id="mart_network_hour",
    update_cols=[
        "service_date",
        "lines_reporting",
        "lines_impacted",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_good_service_pct",
    ],
)

network_day_df = spark.sql(
    f"""
SELECT
  d.service_date,
  count(distinct d.line_id) AS lines_reporting,
  count(distinct CASE WHEN d.disruption_seconds > 0 THEN d.line_id END) AS lines_impacted,
  sum(d.total_seconds) AS total_seconds,
  sum(d.good_service_seconds) AS good_service_seconds,
  sum(d.disruption_seconds) AS disruption_seconds,
  sum(d.severity_weighted_seconds) AS severity_weighted_seconds,
  CASE WHEN sum(d.total_seconds) > 0
    THEN sum(d.good_service_seconds) / sum(d.total_seconds)
    ELSE NULL END AS network_good_service_pct
FROM {CATALOG}.{GOLD_NS}.mart_line_day d
INNER JOIN affected_days a
  ON d.service_date = a.service_date
GROUP BY d.service_date
"""
)

merge_into_table(
    ss=spark,
    df=network_day_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.mart_network_day",
    on_clause="t.service_date = s.service_date",
    insert_cols=[
        "service_date",
        "lines_reporting",
        "lines_impacted",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_good_service_pct",
    ],
    temp_prefix="tmp_mart_network_day",
    run_id=RUN_ID,
    batch_id="mart_network_day",
    update_cols=[
        "lines_reporting",
        "lines_impacted",
        "total_seconds",
        "good_service_seconds",
        "disruption_seconds",
        "severity_weighted_seconds",
        "network_good_service_pct",
    ],
)

# ----------------------------
# Current state for affected lines only
# ----------------------------
current_status_df = spark.sql(
    f"""
WITH latest AS (
  SELECT
    i.*,
    row_number() OVER (PARTITION BY i.line_id ORDER BY i.valid_from DESC, i.event_ts DESC, i.ingest_ts DESC) AS rn
  FROM {CATALOG}.{GOLD_NS}.fact_line_status_interval i
  INNER JOIN affected_lines a
    ON i.line_id = a.line_id
)
SELECT
  line_id,
  line_name,
  mode,
  status_severity,
  status_desc,
  is_disrupted,
  disruption_category,
  reason,
  valid_from AS status_valid_from,
  valid_to AS status_valid_to,
  event_ts AS last_event_ts,
  ingest_ts AS last_ingest_ts,
  (unix_timestamp(current_timestamp()) - unix_timestamp(valid_from)) / 60.0 AS last_changed_minutes_ago
FROM latest
WHERE rn = 1
"""
)

merge_into_table(
    ss=spark,
    df=current_status_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.current_line_status",
    on_clause="t.line_id = s.line_id",
    insert_cols=[
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
        "last_changed_minutes_ago",
    ],
    temp_prefix="tmp_current_status",
    run_id=RUN_ID,
    batch_id="current_status",
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
        "last_changed_minutes_ago",
    ],
)

# ----------------------------
# ML feature tables for affected lines
# ----------------------------
ml_hour_df = spark.sql(
    f"""
WITH base AS (
  SELECT
    bucket_hour,
    line_id,
    mode,
    hour(bucket_hour) AS hour_of_day,
    dayofweek(bucket_hour) AS day_of_week,
    disruption_seconds,
    good_service_pct,
    state_change_count,
    severity_weighted_seconds,
    CASE WHEN total_seconds > 0 THEN disruption_seconds / total_seconds ELSE NULL END AS disruption_ratio
  FROM {CATALOG}.{GOLD_NS}.mart_line_hour
  WHERE line_id IN (SELECT line_id FROM affected_lines)
), fe AS (
  SELECT
    *,
    lag(disruption_seconds, 1) OVER (PARTITION BY line_id ORDER BY bucket_hour) AS lag_1h_disruption_seconds,
    lag(disruption_seconds, 2) OVER (PARTITION BY line_id ORDER BY bucket_hour) AS lag_2h_disruption_seconds,
    lag(disruption_seconds, 24) OVER (PARTITION BY line_id ORDER BY bucket_hour) AS lag_24h_disruption_seconds,
    avg(disruption_seconds) OVER (PARTITION BY line_id ORDER BY bucket_hour ROWS BETWEEN 5 PRECEDING AND CURRENT ROW) AS rolling_6h_disruption_seconds,
    avg(disruption_seconds) OVER (PARTITION BY line_id ORDER BY bucket_hour ROWS BETWEEN 23 PRECEDING AND CURRENT ROW) AS rolling_24h_disruption_seconds,
    CASE
      WHEN lead(disruption_seconds, 1) OVER (PARTITION BY line_id ORDER BY bucket_hour) > 0 THEN 1
      ELSE 0
    END AS disruption_next_1h_label
  FROM base
)
SELECT *
FROM fe
"""
)

merge_into_table(
    ss=spark,
    df=ml_hour_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.ml_features_line_hour",
    on_clause="t.line_id = s.line_id AND t.bucket_hour = s.bucket_hour",
    insert_cols=[
        "bucket_hour",
        "line_id",
        "mode",
        "hour_of_day",
        "day_of_week",
        "disruption_seconds",
        "good_service_pct",
        "state_change_count",
        "severity_weighted_seconds",
        "disruption_ratio",
        "lag_1h_disruption_seconds",
        "lag_2h_disruption_seconds",
        "lag_24h_disruption_seconds",
        "rolling_6h_disruption_seconds",
        "rolling_24h_disruption_seconds",
        "disruption_next_1h_label",
    ],
    temp_prefix="tmp_ml_hour",
    run_id=RUN_ID,
    batch_id="ml_hour",
    update_cols=[
        "mode",
        "hour_of_day",
        "day_of_week",
        "disruption_seconds",
        "good_service_pct",
        "state_change_count",
        "severity_weighted_seconds",
        "disruption_ratio",
        "lag_1h_disruption_seconds",
        "lag_2h_disruption_seconds",
        "lag_24h_disruption_seconds",
        "rolling_6h_disruption_seconds",
        "rolling_24h_disruption_seconds",
        "disruption_next_1h_label",
    ],
)

ml_day_df = spark.sql(
    f"""
WITH base AS (
  SELECT
    service_date,
    line_id,
    mode,
    dayofweek(service_date) AS day_of_week,
    disruption_seconds / 60.0 AS downtime_minutes,
    good_service_pct,
    num_state_changes,
    severity_weighted_seconds
  FROM {CATALOG}.{GOLD_NS}.mart_line_day
  WHERE line_id IN (SELECT line_id FROM affected_lines)
), fe AS (
  SELECT
    *,
    lag(downtime_minutes, 1) OVER (PARTITION BY line_id ORDER BY service_date) AS lag_1d_downtime_minutes,
    lag(downtime_minutes, 7) OVER (PARTITION BY line_id ORDER BY service_date) AS lag_7d_downtime_minutes,
    avg(downtime_minutes) OVER (PARTITION BY line_id ORDER BY service_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_7d_downtime_minutes,
    lead(downtime_minutes, 1) OVER (PARTITION BY line_id ORDER BY service_date) AS downtime_next_day_minutes_label
  FROM base
)
SELECT *
FROM fe
"""
)

merge_into_table(
    ss=spark,
    df=ml_day_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.ml_features_line_day",
    on_clause="t.line_id = s.line_id AND t.service_date = s.service_date",
    insert_cols=[
        "service_date",
        "line_id",
        "mode",
        "day_of_week",
        "downtime_minutes",
        "good_service_pct",
        "num_state_changes",
        "severity_weighted_seconds",
        "lag_1d_downtime_minutes",
        "lag_7d_downtime_minutes",
        "rolling_7d_downtime_minutes",
        "downtime_next_day_minutes_label",
    ],
    temp_prefix="tmp_ml_day",
    run_id=RUN_ID,
    batch_id="ml_day",
    update_cols=[
        "mode",
        "day_of_week",
        "downtime_minutes",
        "good_service_pct",
        "num_state_changes",
        "severity_weighted_seconds",
        "lag_1d_downtime_minutes",
        "lag_7d_downtime_minutes",
        "rolling_7d_downtime_minutes",
        "downtime_next_day_minutes_label",
    ],
)

ml_incident_df = spark.sql(
    f"""
WITH incidents AS (
  SELECT
    line_id,
    mode,
    valid_from AS incident_start_ts,
    status_severity,
    interval_seconds AS incident_duration_seconds_label
  FROM {CATALOG}.{GOLD_NS}.fact_line_status_interval
  WHERE is_disrupted = true
    AND line_id IN (SELECT line_id FROM affected_lines)
)
SELECT
  i.line_id,
  i.mode,
  i.incident_start_ts,
  i.status_severity,
  hour(i.incident_start_ts) AS hour_of_day,
  dayofweek(i.incident_start_ts) AS day_of_week,
  count(h.bucket_hour) AS recent_24h_incident_count,
  avg(h.disruption_seconds) AS recent_24h_mean_incident_seconds,
  sum(h.state_change_count) AS recent_24h_state_changes,
  i.incident_duration_seconds_label
FROM incidents i
LEFT JOIN {CATALOG}.{GOLD_NS}.mart_line_hour h
  ON i.line_id = h.line_id
 AND h.bucket_hour >= i.incident_start_ts - INTERVAL 24 HOURS
 AND h.bucket_hour < i.incident_start_ts
GROUP BY i.line_id, i.mode, i.incident_start_ts, i.status_severity, i.incident_duration_seconds_label
"""
)

merge_into_table(
    ss=spark,
    df=ml_incident_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.ml_features_incident_start",
    on_clause="t.line_id = s.line_id AND t.incident_start_ts = s.incident_start_ts",
    insert_cols=[
        "line_id",
        "mode",
        "incident_start_ts",
        "status_severity",
        "hour_of_day",
        "day_of_week",
        "recent_24h_incident_count",
        "recent_24h_mean_incident_seconds",
        "recent_24h_state_changes",
        "incident_duration_seconds_label",
    ],
    temp_prefix="tmp_ml_incident",
    run_id=RUN_ID,
    batch_id="ml_incident",
    update_cols=[
        "mode",
        "status_severity",
        "hour_of_day",
        "day_of_week",
        "recent_24h_incident_count",
        "recent_24h_mean_incident_seconds",
        "recent_24h_state_changes",
        "incident_duration_seconds_label",
    ],
)

ml_anomaly_df = spark.sql(
    f"""
WITH baseline AS (
  SELECT
    line_id,
    hour(bucket_hour) AS hour_of_day,
    dayofweek(bucket_hour) AS day_of_week,
    avg(disruption_seconds) AS expected_disruption_seconds,
    stddev_pop(disruption_seconds) AS std_disruption_seconds
  FROM {CATALOG}.{GOLD_NS}.mart_line_hour
  WHERE line_id IN (SELECT line_id FROM affected_lines)
  GROUP BY line_id, hour(bucket_hour), dayofweek(bucket_hour)
), scored AS (
  SELECT
    h.bucket_hour,
    h.line_id,
    h.mode,
    h.disruption_seconds,
    b.expected_disruption_seconds,
    CASE
      WHEN b.std_disruption_seconds IS NULL OR b.std_disruption_seconds = 0 THEN 0.0
      ELSE (h.disruption_seconds - b.expected_disruption_seconds) / b.std_disruption_seconds
    END AS z_score
  FROM {CATALOG}.{GOLD_NS}.mart_line_hour h
  LEFT JOIN baseline b
    ON h.line_id = b.line_id
   AND hour(h.bucket_hour) = b.hour_of_day
   AND dayofweek(h.bucket_hour) = b.day_of_week
  WHERE h.line_id IN (SELECT line_id FROM affected_lines)
)
SELECT
  bucket_hour,
  line_id,
  mode,
  disruption_seconds,
  expected_disruption_seconds,
  z_score,
  CASE WHEN abs(z_score) >= 2.5 THEN true ELSE false END AS is_anomaly,
  CASE
    WHEN z_score >= 2.5 THEN 'Unusual High Downtime'
    WHEN z_score <= -2.5 THEN 'Unusual Low Downtime'
    ELSE 'Normal'
  END AS anomaly_type
FROM scored
"""
)

merge_into_table(
    ss=spark,
    df=ml_anomaly_df,
    table_fqn=f"{CATALOG}.{GOLD_NS}.ml_anomalies_line_hour",
    on_clause="t.line_id = s.line_id AND t.bucket_hour = s.bucket_hour",
    insert_cols=[
        "bucket_hour",
        "line_id",
        "mode",
        "disruption_seconds",
        "expected_disruption_seconds",
        "z_score",
        "is_anomaly",
        "anomaly_type",
    ],
    temp_prefix="tmp_ml_anomaly",
    run_id=RUN_ID,
    batch_id="ml_anomaly",
    update_cols=[
        "mode",
        "disruption_seconds",
        "expected_disruption_seconds",
        "z_score",
        "is_anomaly",
        "anomaly_type",
    ],
)

# ----------------------------
# Advance watermark
# ----------------------------
new_state_df = spark.sql(
    f"""
SELECT
  '{PIPELINE_NAME}' AS pipeline_name,
  max(ingest_ts) AS last_ingest_ts,
  current_timestamp() AS updated_at
FROM delta_events
"""
)

merge_into_table(
    ss=spark,
    df=new_state_df,
    table_fqn=STATE_TABLE,
    on_clause="t.pipeline_name = s.pipeline_name",
    insert_cols=["pipeline_name", "last_ingest_ts", "updated_at"],
    temp_prefix="tmp_pipeline_state",
    run_id=RUN_ID,
    batch_id="state",
    update_cols=["last_ingest_ts", "updated_at"],
)

spark.stop()
