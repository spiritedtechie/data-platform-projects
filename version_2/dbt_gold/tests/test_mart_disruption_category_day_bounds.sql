-- Fail if daily disruption category metrics are out of valid bounds.
select
  service_date,
  disruption_category,
  incidents_started,
  closed_incidents,
  open_incidents,
  lines_impacted,
  incident_duration_seconds_sum,
  mean_incident_duration_seconds,
  p50_incident_duration_seconds,
  p95_incident_duration_seconds,
  max_incident_duration_seconds,
  recovery_rate
from {{ ref('mart_disruption_category_day') }}
where incidents_started < 0
   or closed_incidents < 0
   or open_incidents < 0
   or lines_impacted < 0
   or closed_incidents > incidents_started
   or open_incidents > incidents_started
   or incidents_started <> (closed_incidents + open_incidents)
   or incident_duration_seconds_sum < 0
   or (mean_incident_duration_seconds is not null and mean_incident_duration_seconds < 0)
   or (p50_incident_duration_seconds is not null and p50_incident_duration_seconds < 0)
   or (p95_incident_duration_seconds is not null and p95_incident_duration_seconds < 0)
   or (max_incident_duration_seconds is not null and max_incident_duration_seconds < 0)
   or (recovery_rate is not null and (recovery_rate < 0 or recovery_rate > 1))
