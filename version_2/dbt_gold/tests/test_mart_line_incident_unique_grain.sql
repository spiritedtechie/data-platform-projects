-- Fail if mart_line_incident has duplicate rows at its declared grain.
select
  line_id,
  incident_start_ts,
  count(*) as row_count
from {{ ref('mart_line_incident') }}
group by line_id, incident_start_ts
having count(*) > 1
