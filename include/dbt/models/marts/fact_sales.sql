{{ 
	config(
        materialized='incremental',
        incremental_strategy= 'merge',
		unique_key= 'invoice_id'
	)
}}
WITH fct_sales_cte AS (
    SELECT
        invoice_no AS invoice_id,
        FORMAT_DATETIME('%Y%m%d%H%M', invoice_datetime) AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['stock_code', 'description', 'unit_price']) }} as product_id,
        {{ dbt_utils.generate_surrogate_key(['customer_id', 'country']) }} as customer_id,
        Quantity AS quantity,
        Quantity * unit_price AS total_amount
    FROM {{ source('staging', 'stg_invoices') }}
    WHERE Quantity > 0
)
SELECT
    invoice_id,
    dt.datetime_id,
    dp.product_id,
    dc.customer_id,
    quantity,
    total_amount
FROM fct_sales_cte fi
INNER JOIN {{ ref('dim_datetime') }} dt 
    ON fi.datetime_id = dt.datetime_id
INNER JOIN {{ ref('dim_product') }} dp 
    ON fi.product_id = dp.product_id
INNER JOIN {{ ref('dim_customer') }} dc 
    ON fi.customer_id = dc.customer_id

{% if is_incremental() %}
WHERE invoice_id NOT IN (SELECT invoice_id FROM {{ this }} )
{% endif %}