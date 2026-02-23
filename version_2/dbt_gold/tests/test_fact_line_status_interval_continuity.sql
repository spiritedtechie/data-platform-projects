with ordered as (
    select
        line_id,
        valid_from,
        valid_to,
        lead(valid_from) over (
            partition by line_id
            order by valid_from, event_ts, ingest_ts, event_id
        ) as next_valid_from
    from {{ ref('fact_line_status_interval') }}
)

select
    line_id,
    valid_from,
    valid_to,
    next_valid_from
from ordered
where next_valid_from is not null
  and valid_to != next_valid_from
