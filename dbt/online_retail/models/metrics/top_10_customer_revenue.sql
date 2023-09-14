SELECT
  c.id as customer_id,
  SUM(fi.total) AS total_revenue
FROM {{ ref('fct_invoices') }} fi
JOIN {{ ref('dim_customer') }} c ON fi.customer_id = c.id
GROUP BY c.id
ORDER BY total_revenue DESC
LIMIT 10