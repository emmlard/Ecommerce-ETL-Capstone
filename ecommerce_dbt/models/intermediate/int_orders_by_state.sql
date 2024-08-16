WITH orders_by_state AS (
    SELECT 
        o.id
        , o.customer_id
        , o.status
        , c.state
    FROM
        {{ ref("stg_orders") }} o
    JOIN
        {{ ref("stg_customers") }} c
    ON
        o.customer_id = c.id
    WHERE
        o.status = 'delivered'
)

SELECT
    state
    , COUNT(state) AS order_count
FROM
    orders_by_state
GROUP BY
    state