WITH product_category_name_translation AS (
    SELECT 
        product_category_name AS category_name
        , product_category_name_english AS category_name_english
    FROM {{ source('ecommerce_raw', 'product_category_name_translation') }}
)

SELECT * FROM product_category_name_translation