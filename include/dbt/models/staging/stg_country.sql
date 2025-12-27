SELECT
    Country AS country_name,
    "Alpha-2_code" AS iso,
    "Alpha-3_code" AS iso3,
    {{ dbt.current_timestamp() }} AS created_at
FROM {{ source('raw', 'raw_country') }}
WHERE Country IS NOT NULL AND "Alpha-2_code" IS NOT NULL