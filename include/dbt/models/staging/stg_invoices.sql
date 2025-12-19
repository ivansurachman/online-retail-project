{{ config(materialized='view') }}

SELECT
    InvoiceNo,
    StockCode,
    Description,
    Quantity,
    CASE
      WHEN LENGTH(InvoiceDate) = 16 THEN
        -- Date format: MM/DD/YYYY HH:MM
        PARSE_DATETIME('%m/%d/%Y %H:%M', InvoiceDate)
      WHEN LENGTH(InvoiceDate) <= 14 THEN
        -- Date format: M/D/YY HH:MM
        PARSE_DATETIME('%m/%d/%y %H:%M', InvoiceDate)
      ELSE NULL
    END AS invoice_datetime,
    CAST(UnitPrice AS FLOAT64) AS unit_price,
    CustomerID,
    Country
FROM {{ source('raw', 'raw_invoices') }}
WHERE InvoiceDate IS NOT NULL