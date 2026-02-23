{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['date_key', 'hour_of_day'],
    on_schema_change='sync_all_columns'
  )
}}

with delta as (
    select
        to_date(date_trunc('day', coalesce(valid_from, event_ts))) as date_key,
        ingest_ts as source_ingest_ts
    from {{ ref('stg_silver_line_status_events') }}
    {% if is_incremental() %}
    where ingest_ts > (select coalesce(max(source_ingest_ts), timestamp('1900-01-01')) from {{ this }})
    {% endif %}
), 
impacted_dates as (
    select
        date_key,
        max(source_ingest_ts) as source_ingest_ts
    from delta
    group by date_key
), 
hours as (
    select explode(sequence(0, 23)) as hour_of_day
)

select
    d.date_key,
    h.hour_of_day,
    dayofweek(d.date_key) as day_of_week,
    date_format(d.date_key, 'EEEE') as day_name,
    weekofyear(d.date_key) as week_of_year,
    month(d.date_key) as month_num,
    date_format(d.date_key, 'MMMM') as month_name,
    quarter(d.date_key) as quarter_num,
    year(d.date_key) as year_num,
    {{ is_weekend_from_date('d.date_key') }} as is_weekend,
    d.source_ingest_ts as source_ingest_ts
from impacted_dates d
cross join hours h
