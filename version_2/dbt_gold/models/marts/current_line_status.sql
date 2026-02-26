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
        relation=ref('fact_line_status_change'),
        key_col='line_id',
        source_watermark_col='source_ingest_ts',
        target_relation=this,
        target_watermark_col='last_ingest_ts',
        lookback_hours=24
    ) }}
),

status_changes as (
    select
        c.line_id,
        c.status_severity as status_severity,
        not {{ is_good_service_from_severity('c.status_severity') }} as is_disrupted,
        coalesce(c.disruption_category, 'Unknown') as disruption_category,
        coalesce(c.reason, 'Unknown') as reason,
        c.source_ingest_ts as ingest_ts,
        c.status_valid_from as status_valid_from,
        coalesce(c.status_valid_to, current_timestamp()) as status_valid_to
    from {{ ref('fact_line_status_change') }} as c
    inner join changed_lines as l on c.line_id = l.line_id
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
            over (partition by line_id order by status_valid_from desc, ingest_ts desc, status_severity asc)
            as rn
    from status_changes
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
    r.status_valid_from,
    r.status_valid_to,
    r.ingest_ts as last_ingest_ts,
    (unix_timestamp(current_timestamp()) - unix_timestamp(r.status_valid_from)) / 60.0 as last_changed_minutes_ago
from ranked as r
left join line_dim as d on r.line_id = d.line_id
left join status_dim as s on r.status_severity = s.status_severity
where r.rn = 1
