{{ config(materialized='table') }}

SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'unit_price']) }} as product_id,
		StockCode AS stock_code,
    Description AS description,
    unit_price AS price
FROM {{ ref('stg_invoices') }}
WHERE StockCode IS NOT NULL
AND unit_price > 0