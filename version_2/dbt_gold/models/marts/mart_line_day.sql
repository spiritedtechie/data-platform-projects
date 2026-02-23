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
        relation=ref('fact_line_status_interval'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
), intervals as (
    select *
    from (
        select
            f.*,
            row_number() over (
                partition by f.line_id, f.valid_from
                order by f.ingest_ts desc, f.event_ts desc, f.event_id desc
            ) as rn
        from {{ ref('fact_line_status_interval') }} f
        inner join changed_lines l on f.line_id = l.line_id
    ) deduped
    where rn = 1
), line_dim as (
    select line_id, line_name, mode
    from {{ ref('dim_line') }}
), status_dim as (
    select * from {{ ref('dim_status') }}
), changes as (
    select
        line_id,
        to_date(change_ts) as service_date,
        count(*) as num_state_changes,
        avg(time_in_new_state_seconds) as avg_time_in_state_seconds,
        avg(case when recovery_flag then time_since_prev_seconds end) as time_to_recover_avg_seconds,
        max(source_ingest_ts) as max_change_ingest_ts
    from {{ ref('fact_line_status_change') }}
    where line_id in (select line_id from changed_lines)
    group by line_id, to_date(change_ts)
), incidents as (
    select
        line_id,
        to_date(valid_from) as service_date,
        avg(interval_seconds) as mean_incident_duration_seconds,
        percentile_approx(interval_seconds, 0.5) as p50_incident_duration_seconds,
        percentile_approx(interval_seconds, 0.95) as p95_incident_duration_seconds,
        max(interval_seconds) as max_incident_duration_seconds,
        max(ingest_ts) as max_incident_ingest_ts
    from intervals
    where is_disrupted = true
    group by line_id, to_date(valid_from)
), expanded as (
    select
        i.*,
        explode(sequence(date_trunc('day', i.valid_from), date_trunc('day', i.valid_to), interval 1 day)) as bucket_day
    from intervals i
), overlap as (
    select
        line_id,
        status_severity,
        is_good_service,
        is_disrupted,
        ingest_ts,
        to_date(bucket_day) as service_date,
        greatest(valid_from, bucket_day) as overlap_start,
        least(valid_to, bucket_day + interval 1 day) as overlap_end
    from expanded
), scored as (
    select
        o.*,
        cast(unix_timestamp(overlap_end) - unix_timestamp(overlap_start) as bigint) as overlap_seconds,
        coalesce(d.severity_weight, 0.0) as severity_weight
    from overlap o
    left join status_dim d
      on o.status_severity = d.status_severity
    where overlap_end > overlap_start
), aggregated as (
    select
        s.service_date,
        s.line_id,
        max(l.line_name) as line_name,
        max(l.mode) as mode,
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
        c.avg_time_in_state_seconds,
        c.time_to_recover_avg_seconds,
        min(s.status_severity) as worst_status_severity,
        greatest(max(s.ingest_ts), coalesce(max(i.max_incident_ingest_ts), timestamp('1900-01-01')), coalesce(max(c.max_change_ingest_ts), timestamp('1900-01-01'))) as max_source_ingest_ts
    from scored s
    left join line_dim l
      on s.line_id = l.line_id
    left join incidents i
      on s.line_id = i.line_id and s.service_date = i.service_date
    left join changes c
      on s.line_id = c.line_id and s.service_date = c.service_date
    group by s.service_date, s.line_id, i.mean_incident_duration_seconds, i.p50_incident_duration_seconds,
      i.p95_incident_duration_seconds, i.max_incident_duration_seconds, c.num_state_changes,
      c.avg_time_in_state_seconds, c.time_to_recover_avg_seconds
)
select
    service_date,
    line_id,
    line_name,
    mode,
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
    avg_time_in_state_seconds,
    time_to_recover_avg_seconds,
    worst_status_severity,
    {{ safe_divide('least(raw_good_service_seconds, least(raw_total_seconds, 86400))', 'least(raw_total_seconds, 86400)') }} as good_service_pct,
    max_source_ingest_ts
from aggregated
