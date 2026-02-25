with ordered as (
    select
        line_id,
        status_valid_from,
        source_ingest_ts,
        status_severity,
        reason_text_hash,
        lag(status_severity) over (
            partition by line_id
            order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
        ) as prev_new_status_severity,
        lag(reason_text_hash) over (
            partition by line_id
            order by status_valid_from, source_ingest_ts, status_severity, coalesce(reason_text_hash, '')
        ) as prev_new_reason_text_hash
    from {{ ref('fact_line_status_change') }}
)

select
    line_id,
    status_valid_from,
    source_ingest_ts,
    status_severity,
    reason_text_hash,
    prev_new_status_severity,
    prev_new_reason_text_hash
from ordered
where prev_new_status_severity is not null
  and status_severity = prev_new_status_severity
  and (reason_text_hash <=> prev_new_reason_text_hash)
