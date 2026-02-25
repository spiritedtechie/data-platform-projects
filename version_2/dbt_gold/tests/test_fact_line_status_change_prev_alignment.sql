with c as (
    select
        line_id,
        change_ts,
        source_ingest_ts,
        prev_change_ts,
        prev_status_severity,
        prev_reason_text_hash,
        new_status_severity,
        new_reason_text_hash
    from {{ ref('fact_line_status_change') }}
),

prev_lookup as (
    select
        line_id,
        change_ts,
        -- If multiple rows share the same line_id + change_ts, this test cannot
        -- deterministically resolve the single predecessor state.
        count(*) as prev_rows,
        min(new_status_severity) as expected_prev_status_severity,
        min(new_reason_text_hash) as expected_prev_reason_text_hash
    from c
    group by line_id, change_ts
),

line_stats as (
    select
        line_id,
        min(change_ts) as min_change_ts
    from c
    group by line_id
)

select
    c.line_id,
    c.change_ts,
    c.source_ingest_ts,
    c.prev_change_ts,
    c.prev_status_severity,
    p.expected_prev_status_severity,
    c.prev_reason_text_hash,
    p.expected_prev_reason_text_hash,
    p.prev_rows,
    s.min_change_ts
from c
left join prev_lookup as p
    on c.line_id = p.line_id
    and c.prev_change_ts <=> p.change_ts
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
        -- If there are no predecessor rows, the prev_ values should be null and the prev_change_ts should 
        -- be before the first change for the line.
        p.prev_rows is null
        and c.prev_change_ts < s.min_change_ts
    )
)
