{{
  config(
    materialized='table'
  )
}}

select
    f.line_id,
    f.event_id,
    f.event_ts,
    f.source_valid_from,
    f.source_valid_to,
    f.interval_start_ts,
    f.interval_end_ts,
    f.interval_seconds,
    f.status_severity,
    f.is_good_service,
    f.is_disrupted,
    f.disruption_category,
    f.reason,
    f.reason_text_hash,
    f.ingest_ts,
    coalesce(d.severity_weight, 0.0) as severity_weight
from {{ ref('fact_line_status_interval') }} as f
left join {{ ref('dim_status') }} as d
    on f.status_severity = d.status_severity
where f.interval_end_ts is not null
