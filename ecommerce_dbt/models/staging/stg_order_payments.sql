WITH order_payments AS (
    SELECT
        order_id
        , payment_sequential AS sequential
        , payment_type AS type
        , payment_installments AS installments
        , payment_value AS value
    FROM {{ source('ecommerce_raw', 'order_payments') }}
)

SELECT * FROM order_payments