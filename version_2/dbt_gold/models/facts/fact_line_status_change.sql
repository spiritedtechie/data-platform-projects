{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'status_valid_from', 'status_severity', 'reason_text_hash'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('fact_line_status_interval'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='source_ingest_ts',
        lookback_hours=24
    ) }}
),

intervals as (
    select f.*
    from {{ ref('fact_line_status_interval') }} as f
    inner join changed_lines as l on f.line_id = l.line_id
),

ordered as (
    -- previous status_severity and reason_text_hash are calculated using LAG functions ordered by 
    -- interval_start_ts, ingest_ts, status_severity, and reason_text_hash to ensure a consistent order of rows.
    select
        ingest_ts as source_ingest_ts,
        line_id,
        interval_start_ts as status_valid_from,
        interval_end_ts as status_valid_to,
        interval_seconds,
        status_severity as status_severity,
        is_good_service as is_good_service,
        is_disrupted as is_disrupted,
        reason as reason,
        disruption_category as disruption_category,
        reason_text_hash as reason_text_hash,
        lag(status_severity)
            over (
                partition by line_id
                order by interval_start_ts, ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_status_severity,
        lag(reason_text_hash)
            over (
                partition by line_id
                order by interval_start_ts, ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_reason_text_hash
    from intervals
),

state_runs as (
    -- Assign a unique state_run_id to each sequence of rows for a line_id where the status_severity 
    -- and reason_text_hash are unchanged.
    select
        *,
        sum(
            case
                when prev_status_severity is null then 1
                when status_severity <> prev_status_severity then 1
                when coalesce(reason_text_hash, '') <> coalesce(prev_reason_text_hash, '') then 1
                else 0
            end
        ) over (
            partition by line_id
            order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
            rows between unbounded preceding and current row
        ) as state_run_id
    from ordered
),

collapsed as (
    -- Collapse each state run into a single row, calculating the time spent in the new state and ensuring that if any 
    -- interval_seconds within the state run is null, the entire time_in_new_state_seconds is set to null.
    select
        max(source_ingest_ts) as source_ingest_ts,
        line_id,
        min(status_valid_from) as status_valid_from,
        max(status_valid_to) as status_valid_to,
        max(status_severity) as status_severity,
        max_by(is_good_service, source_ingest_ts) as is_good_service,
        max_by(is_disrupted, source_ingest_ts) as is_disrupted,
        max_by(reason, source_ingest_ts) as reason,
        max_by(disruption_category, source_ingest_ts) as disruption_category,
        max(reason_text_hash) as reason_text_hash,
        case
            when count_if(interval_seconds is null) > 0 then null
            else cast(sum(interval_seconds) as bigint)
        end as time_in_status_seconds
    from state_runs
    group by line_id, state_run_id
),

changes as (
    -- Derive the previous change timestamp, status severity, and reason text hash for each change, 
    -- ordered by change timestamp, source ingest timestamp, new status severity, and new reason text 
    -- hash to ensure a consistent order of rows.
    select
        source_ingest_ts,
        line_id,
        status_valid_from,
        status_valid_to,
        time_in_status_seconds,
        status_severity,
        is_good_service,
        is_disrupted,
        reason,
        disruption_category,
        reason_text_hash,
        lag(status_valid_from)
            over (
                partition by line_id
                order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_status_valid_from,
        lag(status_severity)
            over (
                partition by line_id
                order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_status_severity,
        lag(reason_text_hash)
            over (
                partition by line_id
                order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_reason_text_hash
    from collapsed
)

select
    source_ingest_ts,
    line_id,
    status_valid_from,
    status_valid_to,
    time_in_status_seconds,
    status_severity,
    is_good_service,
    is_disrupted,
    reason,
    reason_text_hash,
    disruption_category,
    prev_status_valid_from,
    prev_status_severity,
    prev_reason_text_hash,
    cast(
        case
            when prev_status_valid_from is null then null else unix_timestamp(status_valid_from) - unix_timestamp(prev_status_valid_from)
        end as bigint
    ) as time_since_prev_status_seconds,
    {{ recovery_flag_from_severity('prev_status_severity', 'status_severity') }} as recovery_flag
from changes
