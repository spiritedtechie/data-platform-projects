select
    line_id,
    status_valid_from,
    prev_status_valid_from,
    time_since_prev_status_seconds,
    time_in_status_seconds,
    source_ingest_ts
from {{ ref('fact_line_status_change') }}
where (prev_status_valid_from is not null and time_since_prev_status_seconds is null)
   or time_since_prev_status_seconds < 0
   or time_in_status_seconds < 1
