-- Fail if intervals are non-positive or status severity is outside expected TfL bounds.
select
  line_id,
  status_severity,
  interval_start_ts,
  interval_end_ts,
  interval_seconds
from {{ ref('fact_line_status_interval') }}
where interval_seconds < 1
   or (interval_end_ts is not null and interval_end_ts <= interval_start_ts)
   or status_severity < 0
   or status_severity > 20
