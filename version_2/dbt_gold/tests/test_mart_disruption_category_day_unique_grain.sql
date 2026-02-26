-- Fail if mart_disruption_category_day has duplicate rows at its declared grain.
select
  service_date,
  disruption_category,
  count(*) as row_count
from {{ ref('mart_disruption_category_day') }}
group by service_date, disruption_category
having count(*) > 1
