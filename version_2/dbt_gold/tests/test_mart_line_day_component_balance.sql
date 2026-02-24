-- Fail if daily line totals are not consistent with component seconds.
select
  service_date,
  line_id,
  total_seconds,
  good_service_seconds,
  disruption_seconds
from {{ ref('mart_line_day') }}
where abs(total_seconds - (good_service_seconds + disruption_seconds)) > 1
