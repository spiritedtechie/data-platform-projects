{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'bucket_hour'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_hour'),
        key_col='line_id',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='source_ingest_ts'
    ) }}
), scoped as (
    select *
    from {{ ref('mart_line_hour') }}
    where line_id in (select line_id from changed_lines)
), baseline as (
    select
        line_id,
        hour(bucket_hour) as hour_of_day,
        dayofweek(bucket_hour) as day_of_week,
        avg(disruption_seconds) as expected_disruption_seconds,
        stddev_pop(disruption_seconds) as std_disruption_seconds
    from scoped
    group by line_id, hour(bucket_hour), dayofweek(bucket_hour)
), scored as (
    select
        h.bucket_hour,
        h.line_id,
        h.mode,
        h.disruption_seconds,
        b.expected_disruption_seconds,
        case
            when b.std_disruption_seconds is null or b.std_disruption_seconds = 0 then 0.0
            else (h.disruption_seconds - b.expected_disruption_seconds) / b.std_disruption_seconds
        end as z_score,
        h.max_source_ingest_ts as source_ingest_ts
    from scoped h
    left join baseline b
      on h.line_id = b.line_id
     and hour(h.bucket_hour) = b.hour_of_day
     and dayofweek(h.bucket_hour) = b.day_of_week
)
select
    bucket_hour,
    line_id,
    mode,
    disruption_seconds,
    expected_disruption_seconds,
    z_score,
    case when abs(z_score) >= 2.5 then true else false end as is_anomaly,
    case
        when z_score >= 2.5 then 'Unusual High Downtime'
        when z_score <= -2.5 then 'Unusual Low Downtime'
        else 'Normal'
    end as anomaly_type,
    source_ingest_ts
from scored
