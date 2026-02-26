-- Fail if incident metrics are out of valid bounds.
select
  line_id,
  incident_start_ts,
  incident_end_ts,
  incident_duration_seconds,
  status_runs_in_incident,
  worst_status_severity,
  incident_closed
from {{ ref('mart_line_incident') }}
where status_runs_in_incident <= 0
   or worst_status_severity is null
   or incident_start_ts is null
   or (incident_end_ts is not null and incident_end_ts <= incident_start_ts)
   or (incident_duration_seconds is not null and incident_duration_seconds <= 0)
   or (incident_closed = true and incident_end_ts is null)
   or (incident_closed = false and incident_end_ts is not null)
