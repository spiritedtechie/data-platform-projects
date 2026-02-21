-- Batch vs streaming parity checks

-- 1) Interval table row count
select
  'interval_row_count' as metric,
  (select count(*) from local.gold.fact_line_status_interval) as batch_value,
  (select count(*) from local.gold.fact_line_status_interval_stream) as stream_value;

-- 2) Total interval seconds
select
  'interval_seconds_sum' as metric,
  (select sum(interval_seconds) from local.gold.fact_line_status_interval) as batch_value,
  (select sum(interval_seconds) from local.gold.fact_line_status_interval_stream) as stream_value;

-- 3) Hourly downtime parity by day
select
  b.service_date,
  sum(b.disruption_seconds) as batch_disruption_seconds,
  sum(s.disruption_seconds) as stream_disruption_seconds,
  sum(s.disruption_seconds) - sum(b.disruption_seconds) as diff_seconds
from local.gold.mart_line_hour b
join local.gold.mart_line_hour_stream s
  on b.line_id = s.line_id
 and b.bucket_hour = s.bucket_hour
group by b.service_date
order by b.service_date desc;

-- 4) Current state parity by line
select
  coalesce(b.line_id, s.line_id) as line_id,
  b.status_severity as batch_status,
  s.status_severity as stream_status,
  b.status_valid_from as batch_valid_from,
  s.status_valid_from as stream_valid_from
from local.gold.current_line_status b
full outer join local.gold.current_line_status_stream s
  on b.line_id = s.line_id
order by line_id;
