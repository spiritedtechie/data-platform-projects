with ordered as (
    select
        line_id,
        change_ts,
        source_ingest_ts,
        new_status_severity,
        new_reason_text_hash,
        lag(new_status_severity) over (
            partition by line_id
            order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
        ) as prev_new_status_severity,
        lag(new_reason_text_hash) over (
            partition by line_id
            order by change_ts, source_ingest_ts, new_status_severity, coalesce(new_reason_text_hash, '')
        ) as prev_new_reason_text_hash
    from {{ ref('fact_line_status_change') }}
)

select
    line_id,
    change_ts,
    source_ingest_ts,
    new_status_severity,
    new_reason_text_hash,
    prev_new_status_severity,
    prev_new_reason_text_hash
from ordered
where prev_new_status_severity is not null
  and new_status_severity = prev_new_status_severity
  and (new_reason_text_hash <=> prev_new_reason_text_hash)
