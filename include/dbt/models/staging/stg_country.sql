{{ 
    config(
        materialized='view'
    ) 
}}

SELECT
    nicename AS country_name,
    iso,
    iso3,
    {{ dbt.current_timestamp() }} AS created_at
FROM {{ source('raw', 'raw_country') }}
WHERE nicename IS NOT NULL AND iso IS NOT NULL