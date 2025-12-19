{{ config(materialized='view') }}

SELECT
    nicename AS country_name,
    iso
FROM {{ source('raw', 'country') }}
WHERE nicename IS NOT NULL AND iso IS NOT NULL