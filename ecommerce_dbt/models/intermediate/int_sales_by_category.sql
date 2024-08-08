WITH sales AS (
	SELECT
		o.id
		, p.category_name
		, oi.price
	FROM
		{{ ref('stg_orders') }} o
	JOIN
		{{ ref('stg_order_items') }} oi
	ON
		o.id = oi.order_id
	JOIN
		{{ ref('stg_products') }} p
	ON
		oi.product_id = p.id
)

SELECT
	category_name
	, SUM(price) AS total_sales
FROM
	sales
GROUP BY
	category_name