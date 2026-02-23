{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='line_id',
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('stg_silver_line_status_events'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='last_ingest_ts'
    ) }}
), 
base as (
    select e.*
    from {{ ref('stg_silver_line_status_events') }} e
    inner join changed_lines l on e.line_id = l.line_id
)

select
    line_id,
    max_by(line_name, event_ts) as line_name,
    max_by(mode, event_ts) as mode,
    min(event_ts) as first_seen_ts,
    max(event_ts) as last_seen_ts,
    max(ingest_ts) as last_ingest_ts
from base
group by line_id
