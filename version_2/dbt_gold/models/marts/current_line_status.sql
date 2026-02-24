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
        relation=ref('fact_line_status_interval'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='last_ingest_ts'
    ) }}
),

i as (
    select f.*
    from {{ ref('fact_line_status_interval') }} as f
    inner join changed_lines as l on f.line_id = l.line_id
),

line_dim as (
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
),

status_dim as (
    select
        status_severity,
        status_desc
    from {{ ref('dim_status') }}
),

ranked as (
    select
        *,
        row_number()
            over (partition by line_id order by interval_start_ts desc, ingest_ts desc, status_severity asc)
            as rn
    from i
)

select
    r.line_id,
    d.line_name,
    d.mode,
    r.status_severity,
    s.status_desc,
    r.is_disrupted,
    r.disruption_category,
    r.reason,
    r.interval_start_ts as status_valid_from,
    r.interval_end_ts as status_valid_to,
    r.ingest_ts as last_ingest_ts,
    (unix_timestamp(current_timestamp()) - unix_timestamp(r.interval_start_ts)) / 60.0 as last_changed_minutes_ago
from ranked as r
left join line_dim as d on r.line_id = d.line_id
left join status_dim as s on r.status_severity = s.status_severity
where r.rn = 1
