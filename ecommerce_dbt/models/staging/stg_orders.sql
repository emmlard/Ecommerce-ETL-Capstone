WITH orders AS (
    SELECT
        order_id AS id
        , customer_id
        , order_status AS status
        , order_purchase_timestamp AS purchase_timestamp
        , order_approved_at AS approved_at
        , order_delivered_carrier_date AS delivered_carrier_date
        , order_delivered_customer_date AS delivered_customer_date
        , order_estimated_delivery_date AS estimated_delivery_date
    FROM {{ source('ecommerce_raw', 'orders') }}
)

SELECT * FROM orders