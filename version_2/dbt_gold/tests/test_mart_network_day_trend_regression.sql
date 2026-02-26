-- Fail on large degradation in latest completed day service quality
-- only when there are 2 consecutive bad days.
with latest_complete_date as (
    select max(service_date) as service_date
    from {{ ref('mart_network_day') }}
    where service_date < current_date()
),
daily as (
    select
        service_date,
        network_good_service_pct,
        lines_reporting
    from {{ ref('mart_network_day') }}
    where service_date < current_date()
),
daily_with_baseline as (
    select
        d.service_date,
        d.network_good_service_pct,
        d.lines_reporting,
        (
            select percentile_approx(b.network_good_service_pct, 0.5)
            from daily as b
            where b.service_date < d.service_date
              and b.network_good_service_pct is not null
        ) as baseline_p50,
        (
            select count(*)
            from daily as b
            where b.service_date < d.service_date
              and b.network_good_service_pct is not null
        ) as baseline_days
    from daily as d
),
flagged as (
    select
        *,
        case
            when baseline_days >= 7
                and lines_reporting >= 5
                and network_good_service_pct is not null
                and network_good_service_pct < baseline_p50 - 0.20
                then true
            else false
        end as is_bad_day
    from daily_with_baseline
),
latest_day as (
    select f.*
    from flagged as f
    inner join latest_complete_date as l
        on f.service_date = l.service_date
),
prev_day as (
    select
        f.service_date as prev_service_date,
        f.is_bad_day as prev_is_bad_day
    from flagged as f
)
select
    l.service_date,
    l.network_good_service_pct,
    l.baseline_p50,
    l.baseline_days,
    l.lines_reporting
from latest_day as l
inner join prev_day as p
    on p.prev_service_date = date_sub(l.service_date, 1)
where l.is_bad_day = true
  and p.prev_is_bad_day = true
