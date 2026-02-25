{{
  config(
    materialized='table'
  )
}}

with status_changes as (
    select
        c.line_id,
        c.status_valid_from as status_start_ts,
        c.status_severity as status_severity,
        c.source_ingest_ts as ingest_ts,
        c.time_in_status_seconds as status_duration_seconds
    from {{ ref('fact_line_status_change') }} as c
    where c.time_in_status_seconds is not null
        and c.time_in_status_seconds > 0
)

select
    c.line_id,
    c.status_start_ts,
    cast(from_unixtime(unix_timestamp(c.status_start_ts) + c.status_duration_seconds) as timestamp) as status_end_ts,
    c.status_duration_seconds,
    c.status_severity,
    {{ is_good_service_from_severity('c.status_severity') }} as is_good_service,
    not {{ is_good_service_from_severity('c.status_severity') }} as is_disrupted,
    c.ingest_ts,
    coalesce(d.severity_weight, 0.0) as severity_weight
from status_changes as c
left join {{ ref('dim_status') }} as d
    on c.status_severity = d.status_severity
