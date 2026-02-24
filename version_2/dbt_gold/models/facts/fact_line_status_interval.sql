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

base as (
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        status_severity,
        reason,
        regexp_replace(lower(trim(coalesce(reason, ''))), '\\s+', ' ') as reason_norm,
        sha2(reason_norm, 256) as reason_text_hash,
        coalesce(is_disrupted, not {{ is_good_service_from_severity('status_severity') }}) as is_disrupted
    from events
),

snapshot_resolved as (
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        status_severity,
        reason,
        reason_text_hash,
        is_disrupted
    from (
        select
            *,
            row_number() over (
                partition by line_id, event_ts
                order by
                    -- Prefer more severe statuses.
                    -- For ties, prefer rows with a reason.
                    status_severity asc,
                    case when reason_norm = '' then 1 else 0 end asc,
                    ingest_ts desc,
                    event_id desc
            ) as rn
        from base
    ) as x
    where rn = 1
),

with_prev as (
    select
        *,
        lag(status_severity) over (
            partition by line_id
            order by event_ts, ingest_ts, event_id
        ) as prev_status_severity,
        lag(reason_text_hash) over (
            partition by line_id
            order by event_ts, ingest_ts, event_id
        ) as prev_reason_text_hash
    from snapshot_resolved
),

state_marked as (
    select
        *,
        case
            when prev_status_severity is null then 1
            when status_severity != prev_status_severity then 1
            when reason_text_hash != prev_reason_text_hash then 1
            else 0
        end as is_new_state
    from with_prev
),

state_grouped as (
    select
        *,
        sum(is_new_state) over (
            partition by line_id
            order by event_ts, ingest_ts, event_id
            rows between unbounded preceding and current row
        ) as state_group_id
    from state_marked
),

state_group_first as (
    select
        line_id,
        state_group_id,
        event_id,
        event_ts,
        ingest_ts,
        status_severity,
        reason,
        reason_text_hash,
        is_disrupted
    from (
        select
            *,
            row_number() over (
                partition by line_id, state_group_id
                order by event_ts, ingest_ts, event_id
            ) as rn
        from state_grouped
    ) as x
    where rn = 1
),

state_group_counts as (
    select
        line_id,
        state_group_id,
        cast(count(*) as bigint) as event_count_in_interval
    from state_grouped
    group by line_id, state_group_id
),

segments as (
    select
        f.line_id,
        f.event_id,
        f.event_ts,
        f.ingest_ts,
        f.status_severity,
        f.reason,
        f.reason_text_hash,
        f.is_disrupted,
        f.event_ts as valid_from,
        c.event_count_in_interval,
        lead(f.event_ts) over (
            partition by f.line_id
            order by f.event_ts, f.ingest_ts, f.event_id
        ) as next_valid_from,
        max(f.event_ts) over (partition by f.line_id) as last_change_ts
    from state_group_first as f
    inner join state_group_counts as c
        on
            f.line_id = c.line_id
            and f.state_group_id = c.state_group_id
),

bounded as (
    select
        line_id,
        event_id,
        event_ts,
        ingest_ts,
        valid_from,
        status_severity,
        reason,
        reason_text_hash,
        is_disrupted,
        event_count_in_interval,
        case
            when coalesce(next_valid_from, last_change_ts + interval 1 second) <= valid_from
                then valid_from + interval 1 second
            else coalesce(next_valid_from, last_change_ts + interval 1 second)
        end as valid_to
    from segments
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
    event_count_in_interval
from bounded
