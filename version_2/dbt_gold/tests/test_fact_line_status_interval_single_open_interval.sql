select
    line_id,
    count(*) as open_interval_count
from {{ ref('fact_line_status_interval') }}
where interval_end_ts is null
group by line_id
having count(*) > 1
