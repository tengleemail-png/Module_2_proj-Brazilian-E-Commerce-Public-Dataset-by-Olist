/*
To find the average review score for each product, we can use the following SQL query.
 This query joins the order items with the reviews to calculate the average score for 
 each product.
*/

{{config(
    materialized='table'
)}} 

SELECT 
    oi.product_id,
    AVG(orv.review_score) AS average_review_score
FROM {{ source('brazilian_ecommerce', 'olist_order_items_dataset') }} oi
JOIN {{ source('brazilian_ecommerce', 'olist_order_reviews_dataset') }} orv
ON oi.order_id = orv.order_id
GROUP BY oi.product_id
ORDER BY average_review_score DESC
