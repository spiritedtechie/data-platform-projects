{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['service_date', 'line_id'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_hour'),
        key_col='line_id',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
),

hourly as (
    select h.*
    from {{ ref('mart_line_hour') }} as h
    inner join changed_lines as l on h.line_id = l.line_id
),

intervals as (
    select i.*
    from {{ ref('int_line_interval_scored') }} as i
    inner join changed_lines as l on i.line_id = l.line_id
),

daily_from_hour as (
    -- Aggregate hourly data to daily level
    select
        h.service_date,
        h.line_id,
        max(h.line_name) as line_name,
        max(h.`mode`) as `mode`,
        sum(h.total_seconds) as raw_total_seconds,
        sum(h.good_service_seconds) as raw_good_service_seconds,
        sum(h.disruption_seconds) as raw_disruption_seconds,
        sum(h.severity_weighted_seconds) as severity_weighted_seconds,
        sum(h.incident_count) as incident_count,
        min(h.worst_status_severity) as worst_status_severity,
        max(h.max_source_ingest_ts) as max_hour_ingest_ts
    from hourly as h
    group by h.service_date, h.line_id
),

changes as (
    -- Aggregate change data to daily level
    select
        c.line_id,
        to_date(c.change_ts) as service_date,
        count(*) as num_state_changes,
        avg(c.time_in_new_state_seconds) as time_in_state_avg_seconds,
        avg(case when c.recovery_flag then c.time_since_prev_seconds end) as time_to_recover_avg_seconds,
        max(c.source_ingest_ts) as max_change_ingest_ts
    from {{ ref('fact_line_status_change') }} as c
    inner join changed_lines as l on c.line_id = l.line_id
    group by c.line_id, to_date(c.change_ts)
),

incidents as (
    -- Aggregate incident data to daily level
    select
        line_id,
        to_date(interval_start_ts) as service_date,
        avg(interval_seconds) as mean_incident_duration_seconds,
        percentile_approx(interval_seconds, 0.5) as p50_incident_duration_seconds,
        percentile_approx(interval_seconds, 0.95) as p95_incident_duration_seconds,
        max(interval_seconds) as max_incident_duration_seconds,
        max(ingest_ts) as max_incident_ingest_ts
    from intervals
    where is_disrupted = true
    group by line_id, to_date(interval_start_ts)
),

aggregated as (
    -- Join daily, change, and incident data together
    select
        d.service_date,
        d.line_id,
        d.line_name,
        d.`mode`,
        d.raw_total_seconds,
        d.raw_good_service_seconds,
        d.raw_disruption_seconds,
        d.severity_weighted_seconds,
        d.incident_count,
        i.mean_incident_duration_seconds,
        i.p50_incident_duration_seconds,
        i.p95_incident_duration_seconds,
        i.max_incident_duration_seconds,
        c.time_in_state_avg_seconds,
        c.time_to_recover_avg_seconds,
        d.worst_status_severity,
        coalesce(c.num_state_changes, 0) as num_state_changes,
        greatest(
            d.max_hour_ingest_ts,
            coalesce(max(i.max_incident_ingest_ts), timestamp('1900-01-01')),
            coalesce(max(c.max_change_ingest_ts), timestamp('1900-01-01'))
        ) as max_source_ingest_ts
    from daily_from_hour as d
    left join incidents as i
        on d.line_id = i.line_id and d.service_date = i.service_date
    left join changes as c
        on d.line_id = c.line_id and d.service_date = c.service_date
    group by
        d.service_date, d.line_id, d.line_name, d.`mode`, d.raw_total_seconds, d.raw_good_service_seconds,
        d.raw_disruption_seconds, d.severity_weighted_seconds, d.incident_count, d.worst_status_severity,
        d.max_hour_ingest_ts, i.mean_incident_duration_seconds, i.p50_incident_duration_seconds,
        i.p95_incident_duration_seconds, i.max_incident_duration_seconds, c.num_state_changes,
        c.time_in_state_avg_seconds, c.time_to_recover_avg_seconds
)

select
    service_date,
    line_id,
    line_name,
    `mode`,
    least(raw_total_seconds, 86400) as total_seconds,
    least(raw_good_service_seconds, least(raw_total_seconds, 86400)) as good_service_seconds,
    least(raw_disruption_seconds, least(raw_total_seconds, 86400)) as disruption_seconds,
    severity_weighted_seconds,
    incident_count,
    mean_incident_duration_seconds,
    p50_incident_duration_seconds,
    p95_incident_duration_seconds,
    max_incident_duration_seconds,
    num_state_changes,
    time_in_state_avg_seconds,
    time_to_recover_avg_seconds,
    worst_status_severity,
    {{ safe_divide(
        'least(raw_good_service_seconds, least(raw_total_seconds, 86400))',
        'least(raw_total_seconds, 86400)'
    ) }} as good_service_pct,
    max_source_ingest_ts
from aggregated
