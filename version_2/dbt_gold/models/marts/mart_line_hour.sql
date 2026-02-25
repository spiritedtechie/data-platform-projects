{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['bucket_hour', 'line_id'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('int_line_hour_aggregate'),
        key_col='line_id',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
),

hourly as (
    -- Get the hourly aggregates for the lines that have changed since the last run
    select h.*
    from {{ ref('int_line_hour_aggregate') }} as h
    inner join changed_lines as l on h.line_id = l.line_id
),

line_dim as (
    -- Get the line details for the lines that have changed since the last run
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
),

changes as (
    -- Count the number of status changes for each line in each hour
    select
        c.line_id,
        date_trunc('hour', c.status_valid_from) as bucket_hour,
        count(*) as state_change_count
    from {{ ref('fact_line_status_change') }} as c
    inner join changed_lines as l on c.line_id = l.line_id
    where c.prev_status_valid_from is not null
    group by c.line_id, date_trunc('hour', c.status_valid_from)
),

aggregated as (
    -- Aggregate the hourly data for each line, joining with the line details and status change counts
    select
        h.bucket_hour,
        h.line_id,
        h.service_date,
        max(l.line_name) as line_name,
        max(l.`mode`) as `mode`,
        max(h.total_seconds) as total_seconds,
        max(h.good_service_seconds) as good_service_seconds,
        max(h.disruption_seconds) as disruption_seconds,
        max(h.severity_weighted_seconds) as severity_weighted_seconds,
        max(h.incident_count) as incident_count,
        max(h.worst_status_severity) as worst_status_severity,
        coalesce(max(c.state_change_count), 0) as state_change_count,
        max(h.max_source_ingest_ts) as max_source_ingest_ts
    from hourly as h
    left join line_dim as l
        on h.line_id = l.line_id
    left join changes as c
        on h.line_id = c.line_id and h.bucket_hour = c.bucket_hour
    group by h.bucket_hour, h.line_id, h.service_date
)

select
    bucket_hour,
    service_date,
    line_id,
    line_name,
    `mode`,
    total_seconds,
    good_service_seconds,
    disruption_seconds,
    severity_weighted_seconds,
    incident_count,
    worst_status_severity,
    state_change_count,
    {{ safe_divide(
        'good_service_seconds',
        'total_seconds'
    ) }} as good_service_pct,
    max_source_ingest_ts
from aggregated
