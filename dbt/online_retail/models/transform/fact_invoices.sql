WITH invoices_cte AS (
    SELECT
        invoice_id,
        invoice_date AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} AS product_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'country']) }} AS customer_id,
        unit_price,
        quantity AS quantity,
        quantity * unit_price AS total_price
    FROM
        {{ ref(
            'stg_invoices'
        ) }}
    WHERE
        quantity > 0
)
SELECT
    fi.invoice_id,
    dt.id AS datetime_id,
    dp.id AS product_id,
    dc.id AS customer_id,
    fi.unit_price,
    fi.quantity,
    fi.total_price
FROM
    invoices_cte fi
    INNER JOIN {{ ref('dim_datetime') }}
    dt
    ON fi.datetime_id = dt.id
    INNER JOIN {{ ref('dim_product') }}
    dp
    ON fi.product_id = dp.id
    INNER JOIN {{ ref('dim_customer') }}
    dc
    ON fi.customer_id = dc.id
