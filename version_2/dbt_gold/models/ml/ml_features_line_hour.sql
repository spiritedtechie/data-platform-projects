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
), base as (
    select
        bucket_hour,
        line_id,
        mode,
        hour(bucket_hour) as hour_of_day,
        dayofweek(bucket_hour) as day_of_week,
        disruption_seconds,
        good_service_pct,
        state_change_count,
        severity_weighted_seconds,
        {{ safe_divide('disruption_seconds', 'total_seconds') }} as disruption_ratio,
        max_source_ingest_ts as source_ingest_ts
    from {{ ref('mart_line_hour') }}
    where line_id in (select line_id from changed_lines)
), fe as (
    select
        *,
        lag(disruption_seconds, 1) over (partition by line_id order by bucket_hour) as lag_1h_disruption_seconds,
        lag(disruption_seconds, 2) over (partition by line_id order by bucket_hour) as lag_2h_disruption_seconds,
        lag(disruption_seconds, 24) over (partition by line_id order by bucket_hour) as lag_24h_disruption_seconds,
        avg(disruption_seconds) over (partition by line_id order by bucket_hour rows between 5 preceding and current row) as rolling_6h_disruption_seconds,
        avg(disruption_seconds) over (partition by line_id order by bucket_hour rows between 23 preceding and current row) as rolling_24h_disruption_seconds,
        case when lead(disruption_seconds, 1) over (partition by line_id order by bucket_hour) > 0 then 1 else 0 end as disruption_next_1h_label
    from base
)
select * from fe
