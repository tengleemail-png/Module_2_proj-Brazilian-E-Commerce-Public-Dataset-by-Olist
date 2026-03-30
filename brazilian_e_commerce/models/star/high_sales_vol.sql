/*
to find the products with the highest sales volume, we can use the following SQL query.
 This query sums the quantity of each product ordered.
*/

{{config(
    materialized='table'
)}} 

SELECT 
    oi.product_id,
    SUM(oi.order_item_id) AS total_sales_volume
FROM {{ source('brazilian_ecommerce', 'olist_order_items_dataset') }} oi
GROUP BY oi.product_id
ORDER BY total_sales_volume DESC