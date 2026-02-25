select
    line_id,
    interval_start_ts,
    count(*) as row_count
from {{ ref('fact_line_status_interval') }}
group by line_id, interval_start_ts
having count(*) > 1
