-- Fail on large degradation in latest day service quality vs trailing baseline.
with latest_day as (
    select service_date, network_good_service_pct, lines_reporting
    from {{ ref('mart_network_day') }}
    where service_date = (select max(service_date) from {{ ref('mart_network_day') }})
), baseline as (
    select
      percentile_approx(network_good_service_pct, 0.5) as baseline_p50,
      count(*) as baseline_days
    from {{ ref('mart_network_day') }}
    where service_date < (select max(service_date) from {{ ref('mart_network_day') }})
      and network_good_service_pct is not null
)
select
  l.service_date,
  l.network_good_service_pct,
  b.baseline_p50,
  b.baseline_days,
  l.lines_reporting
from latest_day l
cross join baseline b
where b.baseline_days >= 7
  and l.lines_reporting >= 5
  and l.network_good_service_pct is not null
  and l.network_good_service_pct < b.baseline_p50 - 0.20
