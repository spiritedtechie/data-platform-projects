{{ config(materialized='table') }}

select 
    cast(invoiceno as varchar(10)) as invoice_id,
    cast(stockcode as varchar(10)) as stock_code,
    cast(description as varchar(100)),
    cast(quantity as integer),
    to_timestamp(invoicedate,'MM/DD/YY HH24:MI') as invoice_date,
    cast(unitprice as decimal) as unit_price,
    cast(customerid as integer) as customer_id,
    cast(country as varchar(50))
from {{ source('raw_retail', 'invoices') }}