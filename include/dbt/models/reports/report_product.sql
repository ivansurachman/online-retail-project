SELECT
  dp.product_id,
  dp.stock_code,
  dp.description,
  SUM(fs.quantity) AS total_quantity_sold
FROM {{ ref('fact_sales') }} fs
JOIN {{ ref('dim_product') }} dp ON fs.product_id = dp.product_id
GROUP BY dp.product_id, dp.stock_code, dp.description
ORDER BY total_quantity_sold DESC
LIMIT 10