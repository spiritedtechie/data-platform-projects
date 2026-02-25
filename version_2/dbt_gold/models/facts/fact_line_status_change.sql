{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'change_ts', 'new_status_severity'],
    on_schema_change='sync_all_columns'
  )
}}

with changed_lines as (
    {{ incremental_distinct_keys(
        relation=ref('fact_line_status_interval'),
        key_col='line_id',
        source_watermark_col='ingest_ts',
        target_relation=this,
        target_watermark_col='source_ingest_ts'
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
        line_id,
        interval_start_ts as change_ts,
        interval_seconds,
        ingest_ts as source_ingest_ts,
        status_severity as new_status_severity,
        reason_text_hash as new_reason_text_hash,
        lag(status_severity)
            over (
                partition by line_id
                order by interval_start_ts, ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_raw_status_severity,
        lag(reason_text_hash)
            over (
                partition by line_id
                order by interval_start_ts, ingest_ts, status_severity, coalesce(reason_text_hash, '')
            ) as prev_raw_reason_text_hash
    from intervals
),

state_runs as (
    -- Assign a unique state_run_id to each sequence of rows for a line_id where the status_severity 
    -- and reason_text_hash are unchanged.
    select
        *,
        sum(
            case
                when prev_raw_status_severity is null then 1
                when new_status_severity <> prev_raw_status_severity then 1
                when coalesce(new_reason_text_hash, '') <> coalesce(prev_raw_reason_text_hash, '') then 1
                else 0
            end
        ) over (
            partition by line_id
            order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
            rows between unbounded preceding and current row
        ) as state_run_id
    from ordered
),

collapsed as (
    -- Collapse each state run into a single row, calculating the time spent in the new state and ensuring that if any 
    -- interval_seconds within the state run is null, the entire time_in_new_state_seconds is set to null.
    select
        line_id,
        min(change_ts) as change_ts,
        max(source_ingest_ts) as source_ingest_ts,
        max(new_status_severity) as new_status_severity,
        max(new_reason_text_hash) as new_reason_text_hash,
        case
            when count_if(interval_seconds is null) > 0 then null
            else cast(sum(interval_seconds) as bigint)
        end as time_in_new_state_seconds
    from state_runs
    group by line_id, state_run_id
),

changes as (
    -- Derive the previous change timestamp, status severity, and reason text hash for each change, 
    -- ordered by change timestamp, source ingest timestamp, new status severity, and new reason text 
    -- hash to ensure a consistent order of rows.
    select
        line_id,
        change_ts,
        source_ingest_ts,
        new_status_severity,
        new_reason_text_hash,
        time_in_new_state_seconds,
        lag(change_ts)
            over (
                partition by line_id
                order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
            ) as prev_change_ts,
        lag(new_status_severity)
            over (
                partition by line_id
                order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
            ) as prev_status_severity,
        lag(new_reason_text_hash)
            over (
                partition by line_id
                order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
            ) as prev_reason_text_hash
    from collapsed
)

select
    line_id,
    change_ts,
    prev_change_ts,
    cast(
        case
            when prev_change_ts is null then null else unix_timestamp(change_ts) - unix_timestamp(prev_change_ts)
        end as bigint
    ) as time_since_prev_seconds,
    prev_status_severity,
    new_status_severity,
    prev_reason_text_hash,
    new_reason_text_hash,
    {{ recovery_flag_from_severity('prev_status_severity', 'new_status_severity') }} as recovery_flag,
    time_in_new_state_seconds,
    source_ingest_ts
from changes
where prev_status_severity is not null
