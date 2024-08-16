WITH customers AS (
    SELECT
    	customer_id AS id
        , customer_unique_id AS unique_id
        , customer_zip_code_prefix AS zip_code_prefix
        , customer_city AS city
        , customer_state AS state
    FROM {{ source('ecommerce_raw', 'customers') }}
)

SELECT * FROM customers