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
        relation=ref('fact_line_status_change'),
        key_col='line_id',
        source_watermark_col='source_ingest_ts',
        target_relation=this,
        target_watermark_col='max_source_ingest_ts',
        lookback_hours=24
    ) }}
),

changes as (
    select
        c.line_id,
        c.status_valid_from,
        c.status_valid_to,
        c.time_in_status_seconds,
        c.status_severity,
        c.is_disrupted,
        coalesce(c.disruption_category, 'Unknown') as disruption_category,
        coalesce(c.reason, 'Unknown') as reason,
        c.source_ingest_ts,
        coalesce(c.reason_text_hash, '') as reason_text_hash
    from {{ ref('fact_line_status_change') }} as c
    inner join changed_lines as l on c.line_id = l.line_id
),

ordered as (
    select
        *,
        lag(is_disrupted) over (
            partition by line_id
            order by status_valid_from, source_ingest_ts, status_severity, reason_text_hash
        ) as prev_is_disrupted
    from changes
),

incidentized as (
    -- Build an incident sequence per line when state transitions into disruption.
    select
        *,
        sum(
            case
                when is_disrupted and coalesce(prev_is_disrupted, false) = false then 1
                else 0
            end
        ) over (
            partition by line_id
            order by status_valid_from, source_ingest_ts, status_severity, reason_text_hash
            rows between unbounded preceding and current row
        ) as incident_sequence_number
    from ordered
),

disruption_rows as (
    select *
    from incidentized
    where is_disrupted = true
      and incident_sequence_number > 0
),

collapsed as (
    select
        line_id,
        incident_sequence_number,
        min(status_valid_from) as incident_start_ts,
        case
            when count_if(status_valid_to is null) > 0 then null
            else max(status_valid_to)
        end as incident_end_ts,
        case
            when count_if(time_in_status_seconds is null) > 0 then null
            else cast(sum(time_in_status_seconds) as bigint)
        end as incident_duration_seconds,
        count(*) as status_runs_in_incident,
        min(status_severity) as worst_status_severity,
        max_by(disruption_category, coalesce(time_in_status_seconds, 0)) as primary_disruption_category,
        max_by(disruption_category, status_valid_from) as latest_disruption_category,
        max_by(reason, status_valid_from) as latest_reason,
        max(source_ingest_ts) as max_source_ingest_ts
    from disruption_rows
    group by line_id, incident_sequence_number
),

line_dim as (
    select
        line_id,
        line_name,
        mode
    from {{ ref('dim_line') }}
)

select
    c.line_id,
    d.line_name,
    d.mode,
    to_date(c.incident_start_ts) as service_date,
    c.incident_sequence_number,
    c.incident_start_ts,
    c.incident_end_ts,
    c.incident_duration_seconds,
    c.status_runs_in_incident,
    c.worst_status_severity,
    c.primary_disruption_category,
    c.latest_disruption_category,
    c.latest_reason,
    case when c.incident_end_ts is not null then true else false end as incident_closed,
    c.max_source_ingest_ts
from collapsed as c
left join line_dim as d on c.line_id = d.line_id
