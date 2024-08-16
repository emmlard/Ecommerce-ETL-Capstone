SELECT
    *
FROM
	{{ ref("int_orders_by_state") }}
ORDER BY 
    order_count desc
LIMIT 1