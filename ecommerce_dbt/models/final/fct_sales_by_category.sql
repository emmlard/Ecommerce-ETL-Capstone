SELECT
    *
FROM
	{{ ref("int_sales_by_category") }}
ORDER BY 
    total_sales desc
LIMIT 1