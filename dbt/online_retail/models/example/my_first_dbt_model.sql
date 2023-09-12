{{ config(materialized='table') }}

with invoices_per_country as (
    select country, count(*) as count
    from online_retail
    group by country
)

select *
from invoices_per_country