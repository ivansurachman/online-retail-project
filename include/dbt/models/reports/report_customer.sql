SELECT
  dc.country,
  dc.iso,
  COUNT(DISTINCT fs.invoice_id) AS total_transactions,
  SUM(fs.total_amount) AS total_revenue
FROM {{ ref('fact_sales')}} fs
JOIN {{ ref('dim_customer')}} dc ON fs.customer_id = dc.customer_id
GROUP BY dc.country, dc.iso
ORDER BY total_revenue DESC
LIMIT 10