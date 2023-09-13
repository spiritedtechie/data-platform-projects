SELECT
    DISTINCT {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} AS id,
    stock_code,
    description,
    unit_price
FROM
    {{ ref('stg_invoices') }}
WHERE
    stock_code IS NOT NULL
    AND unit_price > 0
