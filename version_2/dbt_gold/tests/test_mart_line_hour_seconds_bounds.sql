-- Fail if hourly aggregates have impossible second totals.
select
  bucket_hour,
  line_id,
  total_seconds,
  good_service_seconds,
  disruption_seconds
from {{ ref('mart_line_hour') }}
where total_seconds < 0
   or total_seconds > 3600
   or good_service_seconds < 0
   or disruption_seconds < 0
   or good_service_seconds > total_seconds
   or disruption_seconds > total_seconds
