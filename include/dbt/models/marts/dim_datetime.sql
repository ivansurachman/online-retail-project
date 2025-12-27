{{ 
	config(
		unique_key= 'datetime_id'
	)
}}

SELECT DISTINCT
    FORMAT_DATETIME('%Y%m%d%H%M', invoice_datetime) AS datetime_id,
    invoice_datetime,
    DATE(invoice_datetime) AS date,
    EXTRACT(YEAR FROM invoice_datetime) AS year,
    EXTRACT(MONTH FROM invoice_datetime) AS month,
    EXTRACT(DAY FROM invoice_datetime) AS day,
    EXTRACT(HOUR FROM invoice_datetime) AS hour,
    FORMAT_DATETIME('%A', invoice_datetime) AS day_name,
    FORMAT_DATETIME('%B', invoice_datetime) AS month_name
FROM {{ ref('stg_invoices') }}
WHERE invoice_datetime IS NOT NULL