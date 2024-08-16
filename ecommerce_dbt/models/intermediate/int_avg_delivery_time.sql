WITH avg_delivery_time AS (
    SELECT
        id
        , purchase_timestamp
        , delivered_customer_date
    FROM 
        {{ ref("stg_orders") }}
    WHERE
        status = 'delivered'
)

SELECT
    id
    , avg(TIMESTAMP_DIFF(delivered_customer_date, purchase_timestamp, hour)) AS avg_delivery_time_hour
FROM
    avg_delivery_time
WHERE
    delivered_customer_date IS NOT NULL
GROUP BY 
    id


