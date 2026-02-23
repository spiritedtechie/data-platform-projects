{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='service_date',
    on_schema_change='sync_all_columns'
  )
}}

with impacted_days as (
    {{ incremental_distinct_keys(
        relation=ref('mart_line_day'),
        key_col='service_date',
        source_watermark_col='max_source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts'
    ) }}
), scoped as (
    select d.*
    from {{ ref('mart_line_day') }} d
    inner join impacted_days i on d.service_date = i.service_date
)
select
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
group by service_date
