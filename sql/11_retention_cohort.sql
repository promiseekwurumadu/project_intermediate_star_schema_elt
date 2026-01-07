WITH orders AS (
  SELECT
    fs.customer_key,
    dd.year,
    dd.month,
    (dd.year * 12 + dd.month) AS ym_index,
    COUNT(DISTINCT fs.invoiceno) AS orders_in_month
  FROM analytics.fact_sales fs
  JOIN analytics.dim_date dd ON dd.date_key = fs.date_key
  GROUP BY 1, 2, 3, 4
),
first_month AS (
  SELECT
    customer_key,
    MIN(ym_index) AS first_ym_index
  FROM orders
  GROUP BY 1
),
cohorts AS (
  SELECT
    o.customer_key,
    fm.first_ym_index,
    o.ym_index,
    (o.ym_index - fm.first_ym_index) AS month_number
  FROM orders o
  JOIN first_month fm ON fm.customer_key = o.customer_key
)
SELECT
  first_ym_index AS cohort_ym,
  month_number,
  COUNT(DISTINCT customer_key) AS active_customers
FROM cohorts
GROUP BY 1, 2
ORDER BY 1, 2;

