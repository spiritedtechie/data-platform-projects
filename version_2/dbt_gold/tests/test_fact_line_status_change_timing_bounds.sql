select
    line_id,
    change_ts,
    prev_change_ts,
    time_since_prev_seconds,
    time_in_new_state_seconds,
    source_ingest_ts
from {{ ref('fact_line_status_change') }}
where time_since_prev_seconds is null
   or time_since_prev_seconds < 0
   or time_in_new_state_seconds < 1
