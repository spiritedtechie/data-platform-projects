{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'change_ts', 'new_status_severity'],
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
),

i as (
    select f.*
    from {{ ref('fact_line_status_interval') }} as f
    inner join changed_lines as l on f.line_id = l.line_id
),

ordered as (
    select
        line_id,
        interval_start_ts as change_ts,
        interval_end_ts,
        interval_seconds,
        ingest_ts as source_ingest_ts,
        status_severity as new_status_severity,
        lag(status_severity)
            over (partition by line_id order by interval_start_ts, ingest_ts, status_severity)
            as prev_status_severity,
        lag(interval_start_ts)
            over (partition by line_id order by interval_start_ts, ingest_ts, status_severity)
            as prev_change_ts
    from i
)

select
    line_id,
    change_ts,
    prev_change_ts,
    cast(
        case
            when prev_change_ts is null then null else unix_timestamp(change_ts) - unix_timestamp(prev_change_ts)
        end as bigint
    ) as time_since_prev_seconds,
    prev_status_severity,
    new_status_severity,
    {{ recovery_flag_from_severity('prev_status_severity', 'new_status_severity') }} as recovery_flag,
    interval_seconds as time_in_new_state_seconds,
    source_ingest_ts
from ordered
where prev_status_severity is not null
