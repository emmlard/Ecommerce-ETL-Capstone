WITH geolocation AS (
    SELECT
        geolocation_zip_code_prefix AS zip_code_prefix
        , geolocation_lat AS lat
        , geolocation_lng AS lng
        , geolocation_city AS city
        , geolocation_state AS state
    FROM {{ source('ecommerce_raw', 'geolocation') }}
)

SELECT * FROM geolocation