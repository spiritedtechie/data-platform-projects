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
        relation=ref('int_line_status_change_durations'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts',
        lookback_hours=24
    ) }}
),

durations as (
    select d.*
    from {{ ref('int_line_status_change_durations') }} as d
    inner join changed_lines as l on d.line_id = l.line_id
),

expanded as (
    -- Explode status durations into hourly buckets.
    select
        d.*,
        cast(
            from_unixtime(
                unix_timestamp(date_trunc('hour', d.status_start_ts)) + (hour_offset * 3600)
            ) as timestamp
        ) as bucket_hour
    from durations as d
    lateral view explode(
        sequence(
            0,
            cast(
                (unix_timestamp(date_trunc('hour', d.status_end_ts))
                    - unix_timestamp(date_trunc('hour', d.status_start_ts))) / 3600 as int
            )
        )
    ) exploded as hour_offset
),

overlap as (
    -- Calculate overlap between each status duration and hour bucket.
    select
        e.line_id,
        e.status_severity,
        e.is_good_service,
        e.is_disrupted,
        e.ingest_ts,
        e.severity_weight,
        e.bucket_hour,
        greatest(e.status_start_ts, e.bucket_hour) as overlap_start,
        least(
            e.status_end_ts,
            cast(from_unixtime(unix_timestamp(e.bucket_hour) + 3600) as timestamp)
        ) as overlap_end
    from expanded as e
),

scored as (
    -- Filter out rows with no overlap.
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
        sum(s.overlap_seconds) as raw_total_seconds,
        {{ sum_when('s.is_good_service', 's.overlap_seconds') }} as raw_good_service_seconds,
        {{ sum_when('s.is_disrupted', 's.overlap_seconds') }} as raw_disruption_seconds,
        sum(s.overlap_seconds * s.severity_weight) as severity_weighted_seconds,
        count_if(s.is_disrupted) as incident_count,
        min(s.status_severity) as worst_status_severity,
        max(s.ingest_ts) as max_source_ingest_ts
    from scored as s
    group by s.bucket_hour, s.line_id
)

select
    bucket_hour,
    service_date,
    line_id,
    severity_weighted_seconds,
    incident_count,
    worst_status_severity,
    max_source_ingest_ts,
    least(raw_total_seconds, 3600) as total_seconds,
    greatest(
        least(raw_total_seconds, 3600) - least(raw_disruption_seconds, least(raw_total_seconds, 3600)),
        0
    ) as good_service_seconds,
    least(raw_disruption_seconds, least(raw_total_seconds, 3600)) as disruption_seconds
from aggregated
