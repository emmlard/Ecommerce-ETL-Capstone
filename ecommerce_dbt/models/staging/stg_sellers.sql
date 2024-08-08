WITH sellers AS (
    SELECT 
		seller_id AS id
		, seller_zip_code_prefix AS zip_code_prefix
		, seller_city AS city
		, seller_state AS state
    FROM {{ source('ecommerce_raw', 'sellers') }}
)

SELECT * FROM sellers