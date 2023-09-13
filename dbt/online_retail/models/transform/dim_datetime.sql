WITH datetime_cte AS (
  SELECT
    DISTINCT invoice_date AS date_time
  FROM
    {{ ref('stg_invoices') }}
  WHERE
    invoice_date IS NOT NULL
)
SELECT
  date_time AS id,
  date_time AS datetime,
  EXTRACT(
    YEAR
    FROM
      date_time
  ) AS YEAR,
  EXTRACT(
    MONTH
    FROM
      date_time
  ) AS MONTH,
  EXTRACT(
    DAY
    FROM
      date_time
  ) AS DAY,
  EXTRACT(
    HOUR
    FROM
      date_time
  ) AS HOUR,
  EXTRACT(
    MINUTE
    FROM
      date_time
  ) AS MINUTE
FROM
  datetime_cte
