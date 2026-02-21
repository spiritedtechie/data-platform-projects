-- Use Case 1: Reliability score per line (daily)
select
  service_date,
  line_id,
  line_name,
  mode,
  round(good_service_pct * 100, 2) as good_service_pct,
  round(disruption_seconds / 60.0, 2) as downtime_minutes,
  round(severity_weighted_seconds / 60.0, 2) as severity_weighted_minutes
from local.gold.mart_line_day
order by service_date desc, good_service_pct asc;

-- Use Case 2: Peak disruption patterns (hour/day)
select
  line_id,
  hour(bucket_hour) as hour_of_day,
  dayofweek(bucket_hour) as day_of_week,
  avg(disruption_seconds) / 60.0 as avg_disruption_minutes,
  avg(good_service_pct) as avg_good_service_pct
from local.gold.mart_line_hour
group by line_id, hour(bucket_hour), dayofweek(bucket_hour)
order by line_id, day_of_week, hour_of_day;

-- Use Case 3: Incident duration distribution + recovery performance
select
  line_id,
  percentile_approx(interval_seconds, 0.5) / 60.0 as p50_minutes,
  percentile_approx(interval_seconds, 0.95) / 60.0 as p95_minutes,
  avg(interval_seconds) / 60.0 as avg_minutes,
  max(interval_seconds) / 60.0 as max_minutes
from local.gold.fact_line_status_interval
where is_disrupted = true
group by line_id
order by p95_minutes desc;

-- Use Case 4: Volatility / churn
select
  service_date,
  line_id,
  num_state_changes,
  avg_time_in_state_seconds / 60.0 as avg_time_in_state_minutes,
  disruption_seconds / 60.0 as disruption_minutes
from local.gold.mart_line_day
order by service_date desc, num_state_changes desc;

-- Use Case 5: Network live snapshot
select
  line_id,
  line_name,
  mode,
  status_desc,
  status_severity,
  is_disrupted,
  last_changed_minutes_ago,
  reason
from local.gold.current_line_status
order by is_disrupted desc, status_severity asc, line_id;

-- Use Case 6: Severity-weighted impact
select
  service_date,
  line_id,
  round(severity_weighted_seconds / 60.0, 2) as severity_weighted_minutes,
  round(disruption_seconds / 60.0, 2) as raw_downtime_minutes
from local.gold.mart_line_day
order by service_date desc, severity_weighted_minutes desc;

-- Use Case 7: Worst day and post-mortem pack
with ranked as (
  select
    *,
    row_number() over (partition by line_id order by disruption_seconds desc, service_date desc) as rn
  from local.gold.mart_line_day
)
select
  line_id,
  line_name,
  service_date,
  disruption_seconds / 60.0 as downtime_minutes,
  severity_weighted_seconds / 60.0 as weighted_minutes,
  num_state_changes
from ranked
where rn = 1
order by downtime_minutes desc;

-- Post-mortem drill-through for one line/day (replace placeholders)
select
  line_id,
  status_desc,
  status_severity,
  valid_from,
  valid_to,
  interval_seconds / 60.0 as interval_minutes,
  reason
from local.gold.fact_line_status_interval
where line_id = 'central'
  and to_date(valid_from) = date '2026-02-04'
order by valid_from;
