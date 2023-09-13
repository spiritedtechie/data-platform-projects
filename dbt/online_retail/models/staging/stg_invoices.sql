{{ config(
    materialized = 'table'
) }}

-- To do replace country: USA with full length name
SELECT
    CAST(invoiceno AS VARCHAR(10)) AS invoice_id,
    CAST(stockcode AS VARCHAR(10)) AS stock_code,
    CAST(description AS VARCHAR(100)),
    CAST(
        quantity AS INTEGER
    ),
    TO_TIMESTAMP(
        invoicedate,
        'MM/DD/YY HH24:MI'
    ) AS invoice_date,
    CAST(
        unitprice AS DECIMAL
    ) AS unit_price,
    CAST(
        customerid AS INTEGER
    ) AS customer_id,
    CAST(country AS VARCHAR(50))
FROM
    {{ source(
        'raw_retail',
        'invoices'
    ) }}
