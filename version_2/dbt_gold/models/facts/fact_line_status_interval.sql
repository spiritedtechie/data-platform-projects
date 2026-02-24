{{
  config(
    materialized='incremental',
    incremental_strategy='insert_overwrite',
    partition_by=['line_id'],
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

events as (
    select s.*
    from {{ ref('stg_silver_line_status_events') }} as s
    inner join changed_lines as l on s.line_id = l.line_id
),

prepared as (
    -- Normalize reason text, derive disruption flag, and compute interval_start_ts for each event.
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        valid_from as source_valid_from,
        valid_to as source_valid_to,
        status_severity,
        reason,
        regexp_replace(lower(trim(coalesce(reason, ''))), '\\s+', ' ') as reason_normalized,
        sha2(reason_normalized, 256) as reason_text_hash,
        coalesce(is_disrupted, not {{ is_good_service_from_severity('status_severity') }}) as is_disrupted,
        case
            when coalesce(is_disrupted, not {{ is_good_service_from_severity('status_severity') }})
                then coalesce(valid_from, event_ts)
            else event_ts
        end as interval_start_ts
    from events
),

latest_per_start as (
    -- For each line and interval_start_ts, keep only the latest event version.
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        source_valid_from,
        interval_start_ts,
        source_valid_to,
        status_severity,
        reason,
        reason_text_hash,
        is_disrupted
    from (
        select
            *,
            row_number() over (
                partition by line_id, interval_start_ts
                order by event_ts desc, ingest_ts desc, event_id desc
            ) as rn
        from prepared
    ) as x
    where rn = 1
),

sequenced as (
    -- Compute future interval boundaries per line.
    select
        *,
        -- Next interval start for this line.
        lead(interval_start_ts) over (
            partition by line_id
            order by interval_start_ts, ingest_ts, event_id
        ) as next_interval_start_ts
    from latest_per_start
),

finalized as (
    -- handle interval end logic
    select
        *,
        case
            -- If there's a future event, use its interval_start_ts as the end of this interval.
            when next_interval_start_ts is not null
                then next_interval_start_ts
            -- If there's no future event, but this event is a disruption with a valid_to, use that as the interval end.
            when is_disrupted and source_valid_to is not null and source_valid_to > interval_start_ts
                then source_valid_to
        end as interval_end_ts
    from sequenced
)

select
    line_id,
    event_id,
    event_ts,
    source_valid_from,
    source_valid_to,
    interval_start_ts,
    interval_end_ts,
    case
        when interval_end_ts is null then null
        else cast(
            greatest(
                1.0,
                ceil((unix_millis(interval_end_ts) - unix_millis(interval_start_ts)) / 1000.0)
            ) as bigint
        )
    end as interval_seconds,
    status_severity,
    {{ is_good_service_from_severity('status_severity') }} as is_good_service,
    is_disrupted,
    {{ disruption_category_from_reason('reason') }} as disruption_category,
    reason,
    reason_text_hash,
    ingest_ts
from finalized
