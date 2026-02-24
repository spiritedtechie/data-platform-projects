-- Fail if mart_line_hour has duplicate rows at its declared grain.
select
  bucket_hour,
  line_id,
  count(*) as row_count
from {{ ref('mart_line_hour') }}
group by bucket_hour, line_id
having count(*) > 1
