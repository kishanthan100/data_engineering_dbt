WITH sales AS (
    SELECT 
        sales_id,
        customer_sk,
        gross_amount,
        discount_amount,
        net_amount,
        payment_method,
        product_sk

    FROM {{ ref('bronze_sales') }}
    
),

product AS(
    SELECT 
        product_sk
        
    
    FROM {{ ref('bronze_product') }}
),

customer AS (
    SELECT
        customer_sk,
        gender
    FROM {{ ref('bronze_customer') }}
),

joined_query AS(
    SELECT
        sales.sales_id as sales_id,
        sales.gross_amount as gross_amount,
        sales.net_amount as net_amount,
        customer.gender as gender
FROM sales
JOIN product
ON sales.product_sk = product.product_sk

JOIN customer
ON sales.customer_sk = customer.customer_sk
)  

SELECT
    
    gender,
    sum(gross_amount) as total_by_gender
FROM joined_query
GROUP BY 
    gender
    