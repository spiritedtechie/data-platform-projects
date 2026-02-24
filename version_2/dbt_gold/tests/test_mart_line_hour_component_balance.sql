-- Fail if hourly line totals are not consistent with component seconds.
select
  bucket_hour,
  line_id,
  total_seconds,
  good_service_seconds,
  disruption_seconds
from {{ ref('mart_line_hour') }}
where abs(total_seconds - (good_service_seconds + disruption_seconds)) > 1
