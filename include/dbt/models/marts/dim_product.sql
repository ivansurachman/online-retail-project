{{ 
	config(
		materialized='incremental',
    	incremental_strategy= 'merge',
		unique_key= 'product_id'
	)
}}

WITH distinct_products AS (
	SELECT DISTINCT
		stock_code,
		description,
		unit_price
  FROM {{ source('staging', 'stg_invoices') }}
  WHERE stock_code IS NOT NULL
  AND unit_price > 0
)
SELECT 
	{{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} AS product_id,
	stock_code,
	description,
	unit_price
FROM distinct_products

{% if is_incremental() %}
LEFT JOIN {{ this }} existing
	ON p.product_id = existing.product_id
WHERE existing.product_id IS NULL
{% endif %}