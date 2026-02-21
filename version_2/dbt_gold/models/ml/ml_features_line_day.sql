{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'service_date'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_day'),
        key_col='line_id',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='source_ingest_ts'
    ) }}
), base as (
    select
        service_date,
        line_id,
        mode,
        dayofweek(service_date) as day_of_week,
        disruption_seconds / 60.0 as downtime_minutes,
        good_service_pct,
        num_state_changes,
        severity_weighted_seconds,
        max_source_ingest_ts as source_ingest_ts
    from {{ ref('mart_line_day') }}
    where line_id in (select line_id from changed_lines)
), fe as (
    select
        *,
        lag(downtime_minutes, 1) over (partition by line_id order by service_date) as lag_1d_downtime_minutes,
        lag(downtime_minutes, 7) over (partition by line_id order by service_date) as lag_7d_downtime_minutes,
        avg(downtime_minutes) over (partition by line_id order by service_date rows between 6 preceding and current row) as rolling_7d_downtime_minutes,
        lead(downtime_minutes, 1) over (partition by line_id order by service_date) as downtime_next_day_minutes_label
    from base
)
select * from fe
