-- Incremental model for line status events from the source table.
-- It uses merge on line_id, event_id, and status_severity.
-- The on_schema_change option is set to sync_all_columns to handle any changes in the source schema gracefully.
{{
  config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key=['line_id', 'event_id', 'status_severity'],
    on_schema_change='sync_all_columns'
  )
}}

with src as (
    select
        event_id,
        event_ts,
        ingest_ts,
        line_id,
        line_name,
        mode,
        status_severity,
        reason,
        valid_from,
        valid_to,
        is_disrupted,
        payload_hash,
        producer_ingest_ts,
        producer_request_id,
        schema_version,
        coalesce(status_desc, 'Unknown') as status_desc
    from {{ source('silver', 'tfl_line_status_events') }}
    where
        line_id is not null
        {% if is_incremental() %}
            and ingest_ts > (
                select coalesce(max(t.ingest_ts), timestamp('1900-01-01'))
                from {{ this }} as t
            )
        {% endif %}
)

select * from src
