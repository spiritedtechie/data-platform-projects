select
    line_id,
    status_valid_from,
    source_ingest_ts,
    status_severity,
    is_good_service,
    is_disrupted
from {{ ref('fact_line_status_change') }}
where is_good_service is null
   or is_disrupted is null
   or is_disrupted <> (not is_good_service)
