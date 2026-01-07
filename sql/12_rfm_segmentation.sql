WITH customer_stats AS (
  SELECT
    fs.customer_key,
    MAX(dd.date_value) AS last_purchase_date,
    COUNT(DISTINCT fs.invoiceno) AS frequency,
    SUM(fs.revenue) AS monetary
  FROM analytics.fact_sales fs
  JOIN analytics.dim_date dd ON dd.date_key = fs.date_key
  GROUP BY 1
),
max_date AS (
  SELECT MAX(date_value) AS max_date
  FROM analytics.dim_date
),
rfm AS (
  SELECT
    cs.customer_key,
    (md.max_date - cs.last_purchase_date) AS recency_days,
    cs.frequency,
    cs.monetary,
    NTILE(5) OVER (ORDER BY (md.max_date - cs.last_purchase_date) ASC) AS r_score,
    NTILE(5) OVER (ORDER BY cs.frequency DESC) AS f_score,
    NTILE(5) OVER (ORDER BY cs.monetary DESC) AS m_score
  FROM customer_stats cs
  CROSS JOIN max_date md
)
SELECT
  r_score, f_score, m_score,
  COUNT(*) AS customers,
  ROUND(AVG(recency_days)::numeric, 2) AS avg_recency_days,
  ROUND(AVG(frequency)::numeric, 2) AS avg_frequency,
  ROUND(AVG(monetary)::numeric, 2) AS avg_monetary
FROM rfm
GROUP BY 1, 2, 3
ORDER BY r_score DESC, f_score DESC, m_score DESC;

