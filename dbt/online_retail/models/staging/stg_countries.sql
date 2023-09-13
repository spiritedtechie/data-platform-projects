{{ config(materialized='table') }}

with nulls_replaced as (
    select 
        id,
        iso,
        name,
        nicename as nice_name,
        NULLIF(iso3, 'NULL') as iso3,
        NULLIF(numcode, 'NULL') as num_code,
        phonecode as phone_code
    from {{ source('raw_retail', 'countries') }}
)

select 
    cast(id as integer),
    cast(iso as varchar(3)),
    cast(name as varchar(50)),
    cast(nice_name as varchar(50)),
    cast(iso3 as varchar(3)),
    cast(num_code as integer),
    cast(phone_code as integer)
from nulls_replaced