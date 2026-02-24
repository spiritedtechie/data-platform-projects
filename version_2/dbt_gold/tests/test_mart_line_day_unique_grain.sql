-- Fail if mart_line_day has duplicate rows at its declared grain.
select
  service_date,
  line_id,
  count(*) as row_count
from {{ ref('mart_line_day') }}
group by service_date, line_id
having count(*) > 1
