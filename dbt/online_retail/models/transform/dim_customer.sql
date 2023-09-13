-- Create the dimension table
WITH customer_cte AS (
    SELECT
        DISTINCT {{ dbt_utils.generate_surrogate_key(['customer_id', 'country']) }} AS id,
        country AS country
    FROM
        {{ ref('stg_invoices') }}
    WHERE
        customer_id IS NOT NULL
)
SELECT
    t.*,
    cm.iso
FROM
    customer_cte t
    LEFT JOIN {{ ref('stg_countries') }}
    cm
    ON t.country = cm.nice_name
