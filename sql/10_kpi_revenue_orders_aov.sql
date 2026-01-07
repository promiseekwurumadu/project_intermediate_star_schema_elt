SELECT
  COUNT(*) AS line_items,
  COUNT(DISTINCT fs.invoiceno) AS orders,
  COUNT(DISTINCT dc.customer_id) AS customers,
  ROUND(SUM(fs.revenue)::numeric, 2) AS total_revenue,
  ROUND(SUM(fs.revenue)::numeric / NULLIF(COUNT(DISTINCT fs.invoiceno), 0), 2) AS avg_order_value
FROM analytics.fact_sales fs
JOIN analytics.dim_customer dc ON dc.customer_key = fs.customer_key;

