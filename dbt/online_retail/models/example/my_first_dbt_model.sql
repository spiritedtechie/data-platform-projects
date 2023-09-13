{{ config(
    materialized = 'table'
) }}

WITH invoices_per_country AS (

    SELECT
        country,
        COUNT(*) AS COUNT
    FROM
        {{ source(
            'raw_retail',
            'invoices'
        ) }}
    GROUP BY
        country
)
SELECT
    *
FROM
    invoices_per_country
