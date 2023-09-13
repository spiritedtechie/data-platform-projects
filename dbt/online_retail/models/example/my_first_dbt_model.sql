{{ config(materialized='table') }}

with invoices_per_country as (
    select country, count(*) as count
    from {{ source('raw_retail', 'invoices') }}
    group by country
)

select *
from invoices_per_country