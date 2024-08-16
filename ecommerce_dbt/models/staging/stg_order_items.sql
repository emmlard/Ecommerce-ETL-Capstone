WITH order_items AS (
    SELECT
        order_id
        , order_item_id AS item_id
        , product_id
        , seller_id
        , shipping_limit_date
        , price
        , freight_value
    FROM {{ source('ecommerce_raw', 'order_items') }}
)

SELECT * FROM order_items