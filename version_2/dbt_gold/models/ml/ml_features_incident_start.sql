{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'incident_start_ts'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('fact_line_status_interval'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='source_ingest_ts'
    ) }}
), incidents as (
    select
        i.line_id,
        d.mode,
        i.valid_from as incident_start_ts,
        i.status_severity,
        i.interval_seconds as incident_duration_seconds_label,
        i.ingest_ts as source_ingest_ts
    from {{ ref('fact_line_status_interval') }} i
    left join {{ ref('dim_line') }} d
      on i.line_id = d.line_id
    where is_disrupted = true
      and i.line_id in (select line_id from changed_lines)
), ctx as (
    select
        i.line_id,
        i.mode,
        i.incident_start_ts,
        i.status_severity,
        hour(i.incident_start_ts) as hour_of_day,
        dayofweek(i.incident_start_ts) as day_of_week,
        count(h.bucket_hour) as recent_24h_incident_count,
        avg(h.disruption_seconds) as recent_24h_mean_incident_seconds,
        sum(h.state_change_count) as recent_24h_state_changes,
        i.incident_duration_seconds_label,
        i.source_ingest_ts
    from incidents i
    left join {{ ref('mart_line_hour') }} h
      on i.line_id = h.line_id
     and h.bucket_hour >= i.incident_start_ts - interval 24 hours
     and h.bucket_hour < i.incident_start_ts
    group by i.line_id, i.mode, i.incident_start_ts, i.status_severity, i.incident_duration_seconds_label, i.source_ingest_ts
)
select * from ctx
