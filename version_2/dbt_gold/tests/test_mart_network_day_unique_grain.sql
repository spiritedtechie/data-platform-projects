select
    service_date,
    count(*) as row_count
from {{ ref('mart_network_day') }}
group by service_date
having count(*) > 1
