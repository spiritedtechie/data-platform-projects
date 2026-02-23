{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='status_severity',
    on_schema_change='sync_all_columns'
  )
}}

with scoped as (
    select
        status_severity,
        status_desc,
        ingest_ts
    from {{ ref('stg_silver_line_status_events') }}
    {% if is_incremental() %}
    where ingest_ts > (select coalesce(max(last_ingest_ts), timestamp('1900-01-01')) from {{ this }})
    {% endif %}
), 
observed as (
    select distinct status_severity, 
        coalesce(status_desc, 'Unknown') as status_desc, 
        ingest_ts
    from scoped
), 
aggregated as (
    select
        status_severity,
        max_by(status_desc, ingest_ts) as status_desc,
        max(ingest_ts) as last_ingest_ts
    from observed
    group by status_severity
)

select
    status_severity,
    status_desc,
    {{ status_category_from_severity('status_severity') }} as status_category,
    {{ severity_weight_from_severity('status_severity') }} as severity_weight,
    {{ is_good_service_from_severity('status_severity') }} as is_good_service,
    last_ingest_ts
from aggregated
