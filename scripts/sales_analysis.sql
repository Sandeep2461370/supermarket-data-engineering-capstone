--Sales Performance Analysis 

--Q1. Which store has the highest total revenue in 2024?

SELECT 
store_id,
store_name,
location, 
total_revenue
FROM gold_sales_summary
ORDER BY total_revenue DESC
LIMIT 1

--Q2. What are the top 5 best-selling products by quantity?


SELECT
    product_id,
    product_name,
    category,
    SUM(total_quantity_sold) AS total_quantity_sold
FROM gold_product_performance
GROUP BY
    product_id,
    product_name,
    category
ORDER BY total_quantity_sold DESC
LIMIT 5;

--Q3. How does each store rank by revenue and transaction count?


%sql
SELECT
    RANK() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
    store_name,
    location,
    total_revenue,
    total_transactions
FROM gold_sales_summary;


--Q4. Which products generate the highest revenue per store?


WITH ProductRevenueRank AS
(
    SELECT
        store_name,
        location,
        product_name,
        category,
        total_revenue,
        RANK() OVER
        (
            PARTITION BY store_name
            ORDER BY total_revenue DESC
        ) AS revenue_rank
    FROM gold_product_performance
)

SELECT
    store_name,
    location,
    product_name,
    category,
    total_revenue
FROM ProductRevenueRank
WHERE revenue_rank = 1
ORDER BY total_revenue DESC;

--Q5. What is the month-on-month sales trend for each store?


SELECT
    store_name,
    location,
    sales_month,
    total_revenue,
    total_transactions
FROM gold_monthly_sales
ORDER BY
    store_name,
    month_number;

--Q6. Which store has the most diverse product sales?


SELECT
    store_name,
    location,
    COUNT(DISTINCT product_id) AS unique_products_sold
FROM gold_product_performance
GROUP BY
    store_name,
    location
ORDER BY unique_products_sold DESC;


