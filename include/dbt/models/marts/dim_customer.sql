WITH customer_cte AS (
	SELECT DISTINCT
	    {{ dbt_utils.generate_surrogate_key(['customer_id', 'country']) }} as customer_id,
		country
	FROM {{ ref('stg_invoices') }}
	WHERE CustomerID IS NOT NULL
), 
customer_cte_2 AS (
	SELECT
		t.*,
		cm.iso
	FROM customer_cte t
	LEFT JOIN {{ ref('stg_country') }} cm ON t.country = cm.country_name
	LEFT JOIN {{ ref('stg_country')}} cm2 ON t.country = cm2.iso3
)
SELECT
	*
FROM customer_cte_2
