with c as (
    select
        line_id,
        status_valid_from,
        source_ingest_ts,
        prev_status_valid_from,
        prev_status_severity,
        prev_reason_text_hash,
        status_severity,
        reason_text_hash
    from {{ ref('fact_line_status_change') }}
),

prev_lookup as (
    select
        line_id,
        status_valid_from,
        -- If multiple rows share the same line_id + status_valid_from, this test cannot
        -- deterministically resolve the single predecessor state.
        count(*) as prev_rows,
        min(status_severity) as expected_prev_status_severity,
        min(reason_text_hash) as expected_prev_reason_text_hash
    from c
    group by line_id, status_valid_from
),

line_stats as (
    select
        line_id,
        min(status_valid_from) as min_status_valid_from
    from c
    group by line_id
)

select
    c.line_id,
    c.status_valid_from,
    c.source_ingest_ts,
    c.prev_status_valid_from,
    c.prev_status_severity,
    p.expected_prev_status_severity,
    c.prev_reason_text_hash,
    p.expected_prev_reason_text_hash,
    p.prev_rows,
    s.min_status_valid_from
from c
left join prev_lookup as p
    on c.line_id = p.line_id
    and c.prev_status_valid_from <=> p.status_valid_from
left join line_stats as s
    on c.line_id = s.line_id
where not (
    (
        -- If there is exactly one predecessor row, the prev_ values should match the expected_prev_ 
        -- values from that row.
        p.prev_rows = 1
        and c.prev_status_severity <=> p.expected_prev_status_severity
        and c.prev_reason_text_hash <=> p.expected_prev_reason_text_hash
    )
    or (
        -- If there are no predecessor rows, the prev_ values should be null and the prev_status_valid_from should 
        -- be before the first change for the line.
        p.prev_rows is null
        and c.prev_status_valid_from < s.min_status_valid_from
    )
)
