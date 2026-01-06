{{ 
  config(
    materialized='incremental',
    unique_key= 'invoice_no',
    incremental_strategy= 'merge',
    partition_by={
      "field": "invoice_datetime",
      "data_type": "datetime",
      "granularity": "day"
    },
    cluster_by=["country", "customer_id", "stock_code"]
  ) 
}}

WITH source_data AS (
  SELECT
      TRIM(InvoiceNo) AS invoice_no,
      TRIM(StockCode) AS stock_code,
      TRIM(Description) AS description,
      SAFE_CAST(Quantity AS INT64) AS quantity,
      CASE
        WHEN LENGTH(InvoiceDate) = 16 THEN
          -- Date format: MM/DD/YYYY HH:MM
          PARSE_DATETIME('%m/%d/%Y %H:%M', TRIM(InvoiceDate))
        WHEN LENGTH(InvoiceDate) <= 14 THEN
          -- Date format: M/D/YY HH:MM
          PARSE_DATETIME('%m/%d/%y %H:%M', TRIM(InvoiceDate))
        ELSE NULL
      END AS invoice_datetime,
      SAFE_CAST(UnitPrice AS FLOAT64) AS unit_price,
      NULLIF(TRIM(CustomerID), '') AS customer_id,
      TRIM(Country) AS country
  FROM {{ source('raw', 'raw_invoices') }}
  WHERE InvoiceDate IS NOT NULL
    AND InvoiceNo IS NOT NULL
)
SELECT
  *,
  {{ dbt.current_timestamp() }} AS created_at
FROM source_data

{% if is_incremental() %}

  WHERE invoice_datetime > (SELECT COALESCE(MAX(invoice_datetime), '1900-01-01 00:00:00') FROM {{ this }} )

{% endif %}