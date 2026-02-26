select
    bucket_hour,
    count(*) as row_count
from {{ ref('mart_network_hour') }}
group by bucket_hour
having count(*) > 1
