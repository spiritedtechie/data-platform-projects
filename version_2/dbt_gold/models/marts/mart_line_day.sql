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
        relation=ref('int_line_interval_scored'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
),

intervals as (
    select i.*
    from {{ ref('int_line_interval_scored') }} as i
    inner join changed_lines as l on i.line_id = l.line_id
),

line_dim as (
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
),

changes as (
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

expanded as (
    -- Explode intervals into daily buckets to handle multi-day incidents and calculate daily metrics
    select
        i.*,
        explode(
            sequence(date_trunc('day', i.interval_start_ts), date_trunc('day', i.interval_end_ts), interval 1 day)
        ) as bucket_day
    from intervals as i
),

overlap as (
    -- Calculate the overlap between each interval and its corresponding daily bucket
    select
        e.line_id,
        e.status_severity,
        e.is_good_service,
        e.is_disrupted,
        e.ingest_ts,
        e.severity_weight,
        to_date(e.bucket_day) as service_date,
        greatest(e.interval_start_ts, e.bucket_day) as overlap_start,
        least(e.interval_end_ts, e.bucket_day + interval 1 day) as overlap_end
    from expanded as e
),

scored as (
    select
        o.*,
        cast(unix_timestamp(o.overlap_end) - unix_timestamp(o.overlap_start) as bigint) as overlap_seconds
    from overlap as o
    where o.overlap_end > o.overlap_start
),

aggregated as (
    select
        s.service_date,
        s.line_id,
        max(l.line_name) as line_name,
        max(l.`mode`) as `mode`,
        sum(s.overlap_seconds) as raw_total_seconds,
        {{ sum_when('s.is_good_service', 's.overlap_seconds') }} as raw_good_service_seconds,
        {{ sum_when('s.is_disrupted', 's.overlap_seconds') }} as raw_disruption_seconds,
        sum(s.overlap_seconds * s.severity_weight) as severity_weighted_seconds,
        count_if(s.is_disrupted) as incident_count,
        i.mean_incident_duration_seconds,
        i.p50_incident_duration_seconds,
        i.p95_incident_duration_seconds,
        i.max_incident_duration_seconds,
        coalesce(c.num_state_changes, 0) as num_state_changes,
        c.time_in_state_avg_seconds,
        c.time_to_recover_avg_seconds,
        min(s.status_severity) as worst_status_severity,
        greatest(
            max(s.ingest_ts),
            coalesce(max(i.max_incident_ingest_ts), timestamp('1900-01-01')),
            coalesce(max(c.max_change_ingest_ts), timestamp('1900-01-01'))
        ) as max_source_ingest_ts
    from scored as s
    left join line_dim as l
        on s.line_id = l.line_id
    left join incidents as i
        on s.line_id = i.line_id and s.service_date = i.service_date
    left join changes as c
        on s.line_id = c.line_id and s.service_date = c.service_date
    group by
        s.service_date, s.line_id, i.mean_incident_duration_seconds, i.p50_incident_duration_seconds,
        i.p95_incident_duration_seconds, i.max_incident_duration_seconds, c.num_state_changes,
        c.time_in_state_avg_seconds, c.time_to_recover_avg_seconds
)

select
    service_date,
    line_id,
    line_name,
    `mode`,
    least(raw_total_seconds, 86400) as total_seconds,
    -- Cap good service and disruption seconds at the total seconds for the day, 
    -- with a max of 86400 seconds (24 hours)
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
