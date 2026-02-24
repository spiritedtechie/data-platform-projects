{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='bucket_hour',
    on_schema_change='sync_all_columns'
  )
}}

with impacted_hours as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_hour'),
        key_col='bucket_hour',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
),

scoped as (
    select h.*
    from {{ ref('mart_line_hour') }} as h
    inner join impacted_hours as i on h.bucket_hour = i.bucket_hour
)

select
    bucket_hour,
    service_date,
    count(distinct line_id) as lines_reporting,
    {{ count_distinct_when('line_id', 'disruption_seconds > 0') }} as lines_impacted,
    sum(total_seconds) as total_seconds,
    sum(good_service_seconds) as good_service_seconds,
    sum(disruption_seconds) as disruption_seconds,
    sum(severity_weighted_seconds) as severity_weighted_seconds,
    {{ safe_divide('sum(good_service_seconds)', 'sum(total_seconds)') }} as network_good_service_pct,
    max(max_source_ingest_ts) as max_source_ingest_ts
from scoped
group by bucket_hour, service_date
