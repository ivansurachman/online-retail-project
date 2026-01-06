{{ 
	config(
		materialized='incremental',
    	incremental_strategy= 'merge',
		unique_key= 'customer_id'
	)
}}

WITH source_data AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['customer_id', 'country']) }} AS customer_id,
		customer_id AS customer_code,
		country
	FROM {{ source('staging', 'stg_invoices') }}
	WHERE customer_id IS NOT NULL
)
SELECT
	t.customer_id,
	t.customer_code,
	t.country,
	COALESCE(
		c1.iso,  -- Try match by country_name
		c2.iso   -- Fallback to iso3
	) AS iso
FROM source_data t
LEFT JOIN {{ source('staging','stg_country') }} c1 
	ON t.country = c1.country_name
LEFT JOIN {{ source('staging','stg_country') }} c2 
	ON t.country = c2.iso3
	AND c1.country_name IS NULL  -- Only if first join failed

{% if is_incremental() %}
WHERE t.customer_id NOT IN (SELECT customer_id FROM {{ this }})
{% endif %}