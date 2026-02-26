select
    date_key,
    hour_of_day,
    count(*) as row_count
from {{ ref('dim_datetime') }}
group by date_key, hour_of_day
having count(*) > 1
