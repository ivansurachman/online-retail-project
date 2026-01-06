{{ 
  config(
    materialized='table'
  )
}}

SELECT DISTINCT
    TRIM(ISO_Country_Name) AS country_name,
    TRIM(ISO_2_char) AS iso,
    TRIM(ISO_3_char) AS iso3,
    {{ dbt.current_timestamp() }} AS created_at
FROM {{ source('raw', 'raw_country') }}
WHERE ISO_Country_Name IS NOT NULL 
  AND ISO_2_char IS NOT NULL