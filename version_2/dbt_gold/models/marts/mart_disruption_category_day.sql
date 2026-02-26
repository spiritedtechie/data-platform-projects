{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['service_date'],
    on_schema_change='sync_all_columns'
  )
}}

with impacted_days as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_incident'),
        key_col='service_date',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts',
        lookback_hours=24
    ) }}
),

scoped as (
    select i.*
    from {{ ref('mart_line_incident') }} as i
    inner join impacted_days as d on i.service_date = d.service_date
),

aggregated as (
    select
        service_date,
        primary_disruption_category as disruption_category,
        count(*) as incidents_started,
        count_if(incident_closed) as closed_incidents,
        count_if(not incident_closed) as open_incidents,
        count(distinct line_id) as lines_impacted,
        cast(sum(coalesce(incident_duration_seconds, 0)) as bigint) as incident_duration_seconds_sum,
        avg(incident_duration_seconds) as mean_incident_duration_seconds,
        percentile_approx(incident_duration_seconds, 0.5) as p50_incident_duration_seconds,
        percentile_approx(incident_duration_seconds, 0.95) as p95_incident_duration_seconds,
        max(incident_duration_seconds) as max_incident_duration_seconds,
        min(worst_status_severity) as worst_status_severity,
        max(max_source_ingest_ts) as max_source_ingest_ts
    from scoped
    group by service_date, primary_disruption_category
)

select
    service_date,
    disruption_category,
    incidents_started,
    closed_incidents,
    open_incidents,
    lines_impacted,
    incident_duration_seconds_sum,
    mean_incident_duration_seconds,
    p50_incident_duration_seconds,
    p95_incident_duration_seconds,
    max_incident_duration_seconds,
    worst_status_severity,
    {{ safe_divide('closed_incidents', 'incidents_started') }} as recovery_rate,
    max_source_ingest_ts
from aggregated
