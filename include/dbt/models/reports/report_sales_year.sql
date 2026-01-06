SELECT 
    dd.year,
    dd.month,
    COUNT(DISTINCT fs.invoice_id) AS total_transactions,
    SUM(fs.total_amount) AS total_revenue
FROM {{ ref('fact_sales')}} fs
JOIN {{ ref('dim_datetime')}} dd ON fs.datetime_id = dd.datetime_id
GROUP BY dd.year, dd.month
ORDER BY dd.year, dd.month