select
    line_id,
    valid_from,
    valid_to,
    count(*) as row_count
from {{ ref('fact_line_status_interval') }}
group by line_id, valid_from, valid_to
having count(*) > 1
