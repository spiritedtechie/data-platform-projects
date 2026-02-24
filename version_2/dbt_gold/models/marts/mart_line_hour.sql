{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['bucket_hour', 'line_id'],
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
),

intervals as (
    select *
    from (
        select
            f.*,
            row_number() over (
                partition by f.line_id, f.valid_from
                order by f.ingest_ts desc, f.event_ts desc, f.event_id desc
            ) as rn
        from {{ ref('fact_line_status_interval') }} as f
        inner join changed_lines as l on f.line_id = l.line_id
    ) as deduped
    where rn = 1
),

line_dim as (
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
),

status_dim as (
    select * from {{ ref('dim_status') }}
),

changes as (
    select
        c.line_id,
        date_trunc('hour', c.change_ts) as bucket_hour,
        count(*) as state_change_count
    from {{ ref('fact_line_status_change') }} as c
    where c.line_id in (select l.line_id from changed_lines as l)
    group by c.line_id, date_trunc('hour', c.change_ts)
),

expanded as (
    select
        i.*,
        explode(sequence(date_trunc('hour', i.valid_from), date_trunc('hour', i.valid_to), interval 1 hour))
            as bucket_hour
    from intervals as i
),

overlap as (
    select
        e.line_id,
        e.status_severity,
        e.is_good_service,
        e.is_disrupted,
        e.ingest_ts,
        e.bucket_hour,
        greatest(e.valid_from, e.bucket_hour) as overlap_start,
        least(e.valid_to, e.bucket_hour + interval 1 hour) as overlap_end
    from expanded as e
),

scored as (
    select
        o.*,
        cast(unix_timestamp(o.overlap_end) - unix_timestamp(o.overlap_start) as bigint) as overlap_seconds,
        coalesce(d.severity_weight, 0.0) as severity_weight
    from overlap as o
    left join status_dim as d
        on o.status_severity = d.status_severity
    where o.overlap_end > o.overlap_start
),

aggregated as (
    select
        s.bucket_hour,
        s.line_id,
        to_date(s.bucket_hour) as service_date,
        max(l.line_name) as line_name,
        max(l.`mode`) as `mode`,
        sum(s.overlap_seconds) as raw_total_seconds,
        {{ sum_when('s.is_good_service', 's.overlap_seconds') }} as raw_good_service_seconds,
        {{ sum_when('s.is_disrupted', 's.overlap_seconds') }} as raw_disruption_seconds,
        sum(s.overlap_seconds * s.severity_weight) as severity_weighted_seconds,
        count_if(s.is_disrupted) as incident_count,
        coalesce(max(c.state_change_count), 0) as state_change_count,
        max(s.ingest_ts) as max_source_ingest_ts
    from scored as s
    left join line_dim as l
        on s.line_id = l.line_id
    left join changes as c
        on s.line_id = c.line_id and s.bucket_hour = c.bucket_hour
    group by s.bucket_hour, s.line_id
)

select
    bucket_hour,
    service_date,
    line_id,
    line_name,
    `mode`,
    least(raw_total_seconds, 3600) as total_seconds,
    least(raw_good_service_seconds, least(raw_total_seconds, 3600)) as good_service_seconds,
    least(raw_disruption_seconds, least(raw_total_seconds, 3600)) as disruption_seconds,
    severity_weighted_seconds,
    incident_count,
    state_change_count,
    {{ safe_divide(
        'least(raw_good_service_seconds, least(raw_total_seconds, 3600))',
        'least(raw_total_seconds, 3600)'
    ) }} as good_service_pct,
    max_source_ingest_ts
from aggregated
