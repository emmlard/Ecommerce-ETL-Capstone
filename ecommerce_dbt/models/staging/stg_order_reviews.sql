WITH order_reviews AS (
    SELECT
        review_id AS id
        , order_id 
        , review_score AS score
        , review_comment_title AS comment_title
        , review_comment_message AS comment_message
        , review_creation_date AS creation_date
        , review_answer_timestamp AS answer_timestamp
    FROM {{ source('ecommerce_raw', 'order_reviews') }}
)

SELECT * FROM order_reviews