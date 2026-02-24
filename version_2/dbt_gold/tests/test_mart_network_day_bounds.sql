-- Fail if daily network metrics are out of valid bounds.
select
  service_date,
  lines_reporting,
  lines_impacted,
  total_seconds,
  good_service_seconds,
  disruption_seconds,
  network_good_service_pct
from {{ ref('mart_network_day') }}
where lines_reporting < 0
   or lines_impacted < 0
   or lines_impacted > lines_reporting
   or total_seconds < 0
   or good_service_seconds < 0
   or disruption_seconds < 0
   or abs(total_seconds - (good_service_seconds + disruption_seconds)) > 1
   or network_good_service_pct < 0
   or network_good_service_pct > 1
