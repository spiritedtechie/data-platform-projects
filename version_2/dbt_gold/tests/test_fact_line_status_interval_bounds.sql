-- Fail if intervals are non-positive or status severity is outside expected TfL bounds.
select
  line_id,
  event_id,
  status_severity,
  valid_from,
  valid_to,
  interval_seconds
from {{ ref('fact_line_status_interval') }}
where interval_seconds < 1
   or valid_to <= valid_from
   or status_severity < 0
   or status_severity > 20
