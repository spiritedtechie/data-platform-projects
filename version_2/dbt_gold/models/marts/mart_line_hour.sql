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
        relation=ref('int_line_interval_scored'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
),

intervals as (
    select i.*
    from {{ ref('int_line_interval_scored') }} as i
    inner join changed_lines as l on i.line_id = l.line_id
),

line_dim as (
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
),

changes as (
    select
        c.line_id,
        date_trunc('hour', c.change_ts) as bucket_hour,
        count(*) as state_change_count
    from {{ ref('fact_line_status_change') }} as c
    inner join changed_lines as l on c.line_id = l.line_id
    group by c.line_id, date_trunc('hour', c.change_ts)
),

expanded as (
    -- Explode intervals into hourly buckets to allow for accurate aggregation of service status within each hour
    select
        i.*,
        explode(
            sequence(date_trunc('hour', i.interval_start_ts), date_trunc('hour', i.interval_end_ts), interval 1 hour)
        )
            as bucket_hour
    from intervals as i
),

overlap as (
    -- Calculate the overlap of each status interval with its corresponding hourly bucket
    select
        e.line_id,
        e.status_severity,
        e.is_good_service,
        e.is_disrupted,
        e.ingest_ts,
        e.severity_weight,
        e.bucket_hour,
        greatest(e.interval_start_ts, e.bucket_hour) as overlap_start,
        least(e.interval_end_ts, e.bucket_hour + interval 1 hour) as overlap_end
    from expanded as e
),

scored as (
    select
        o.*,
        cast(unix_timestamp(o.overlap_end) - unix_timestamp(o.overlap_start) as bigint) as overlap_seconds
    from overlap as o
    where o.overlap_end > o.overlap_start
),

aggregated as (
    select
        s.bucket_hour,
        s.line_id,
        to_date(s.bucket_hour) as service_date,
        max(l.line_name) as line_name,
        max(l.`mode`) as `mode`,
        sum(s.overlap_seconds) as raw_total_seconds,
        {{ sum_when('s.is_good_service', 's.overlap_seconds') }} as raw_good_service_seconds,
        {{ sum_when('s.is_disrupted', 's.overlap_seconds') }} as raw_disruption_seconds,
        sum(s.overlap_seconds * s.severity_weight) as severity_weighted_seconds,
        count_if(s.is_disrupted) as incident_count,
        coalesce(max(c.state_change_count), 0) as state_change_count,
        max(s.ingest_ts) as max_source_ingest_ts
    from scored as s
    left join line_dim as l
        on s.line_id = l.line_id
    left join changes as c
        on s.line_id = c.line_id and s.bucket_hour = c.bucket_hour
    group by s.bucket_hour, s.line_id
)

select
    bucket_hour,
    service_date,
    line_id,
    line_name,
    `mode`,
    least(raw_total_seconds, 3600) as total_seconds,
    least(raw_good_service_seconds, least(raw_total_seconds, 3600)) as good_service_seconds,
    least(raw_disruption_seconds, least(raw_total_seconds, 3600)) as disruption_seconds,
    severity_weighted_seconds,
    incident_count,
    state_change_count,
    {{ safe_divide(
        'least(raw_good_service_seconds, least(raw_total_seconds, 3600))',
        'least(raw_total_seconds, 3600)'
    ) }} as good_service_pct,
    max_source_ingest_ts
from aggregated
