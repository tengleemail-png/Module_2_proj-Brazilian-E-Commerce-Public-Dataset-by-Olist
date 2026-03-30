/*
 to find the number of repeated customers, we can use the following SQL query. 
 This query counts the number of unique customers (customer_unique_id) who have made more 
 than one order on the same product (product_id). We will join the orders dataset with the 
 customers dataset to get the unique customer IDs and then join the order items dataset to 
 count the number of orders for each customer. We will group the result by product_id to 
 find the number of repeated customers.

*/
{{config(
    materialized='table'
)}} 

SELECT 
    oi.product_id,
    COUNT(DISTINCT c.customer_unique_id) AS repeat_customers
FROM {{ source('brazilian_ecommerce', 'olist_order_items_dataset') }} oi
JOIN {{ source('brazilian_ecommerce', 'olist_orders_dataset') }} o ON oi.order_id = o.order_id
JOIN {{ source('brazilian_ecommerce', 'olist_customers_dataset') }} c ON o.customer_id = c.customer_id
GROUP BY oi.product_id
ORDER BY repeat_customers DESC  

