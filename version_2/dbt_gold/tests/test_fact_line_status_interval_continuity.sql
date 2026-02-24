with ordered as (
    select
        line_id,
        interval_start_ts,
        interval_end_ts,
        lead(interval_start_ts) over (
            partition by line_id
            order by interval_start_ts, ingest_ts, status_severity
        ) as next_valid_from
    from {{ ref('fact_line_status_interval') }}
)

select
    line_id,
    interval_start_ts,
    interval_end_ts,
    next_valid_from
from ordered
where next_valid_from is not null
  and not (interval_end_ts <=> next_valid_from)
