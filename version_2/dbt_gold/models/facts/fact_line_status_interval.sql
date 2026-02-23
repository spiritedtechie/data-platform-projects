{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'event_id', 'status_severity'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('stg_silver_line_status_events'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='ingest_ts'
    ) }}
), 
e as (
    select s.*
    from {{ ref('stg_silver_line_status_events') }} s
    inner join changed_lines l on s.line_id = l.line_id
), 
base as (
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        status_severity,
        reason,
        coalesce(valid_from, event_ts) as interval_start,
        valid_to as interval_end_raw,
        coalesce(is_disrupted, not {{ is_good_service_from_severity('status_severity') }}) as is_disrupted,
        sha2(coalesce(reason, ''), 256) as reason_text_hash
    from e
), 
sequenced as (
    select
        *,
        lead(interval_start) over (partition by line_id order by interval_start, event_ts, ingest_ts) as next_interval_start
    from base
), 
bounded as (
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        status_severity,
        reason,
        reason_text_hash,
        interval_start as valid_from,
        case
            when unix_timestamp(
                least(
                    coalesce(interval_end_raw, timestamp('9999-12-31 23:59:59')),
                    coalesce(next_interval_start, timestamp('9999-12-31 23:59:59')),
                    interval_start + interval 48 hours,
                    current_timestamp()
                )
            ) <= unix_timestamp(interval_start)
                then interval_start + interval 1 second
            else least(
                coalesce(interval_end_raw, timestamp('9999-12-31 23:59:59')),
                coalesce(next_interval_start, timestamp('9999-12-31 23:59:59')),
                interval_start + interval 48 hours,
                current_timestamp()
            )
        end as valid_to,
        is_disrupted
    from sequenced
)

select
    line_id,
    event_id,
    event_ts,
    ingest_ts,
    valid_from,
    valid_to,
    cast(unix_timestamp(valid_to) - unix_timestamp(valid_from) as bigint) as interval_seconds,
    status_severity,
    {{ is_good_service_from_severity('status_severity') }} as is_good_service,
    is_disrupted,
    case
        when lower(coalesce(reason, '')) like '%signal%' then 'Signal Failure'
        when lower(coalesce(reason, '')) like '%train cancellation%' then 'Train Cancellations'
        when lower(coalesce(reason, '')) like '%customer incident%' then 'Customer Incident'
        when lower(coalesce(reason, '')) like '%power%' then 'Power Issue'
        when lower(coalesce(reason, '')) like '%staff%' then 'Staffing'
        when lower(coalesce(reason, '')) like '%weather%' then 'Weather'
        when lower(coalesce(reason, '')) like '%engineering%' then 'Engineering Work'
        when lower(coalesce(reason, '')) like '%planned closure%' then 'Planned Closure'
        when reason is null or trim(reason) = '' then 'Unknown'
        else 'Other'
    end as disruption_category,
    reason,
    reason_text_hash,
    1 as event_count_in_interval
from bounded
