-- Comparative Store Performance Dashboard

-- ==========================================================
-- KPI 1 : Total Revenue by Store
-- ==========================================================

SELECT
    store_name,
    location,
    total_revenue
FROM gold_sales_summary
ORDER BY total_revenue DESC;



-- ==========================================================
-- KPI 2 : Average Transaction Value
-- ==========================================================

SELECT
    store_name,
    average_transaction_value
FROM gold_sales_summary
ORDER BY average_transaction_value DESC;



-- ==========================================================
-- KPI 3 : Number of Unique Products Sold
-- ==========================================================

SELECT
    store_name,
    COUNT(DISTINCT product_id) AS unique_products_sold
FROM gold_product_performance
GROUP BY
    store_name
ORDER BY
    unique_products_sold DESC;



-- ==========================================================
-- KPI 4 : Inventory Turnover Ratio
-- ==========================================================

SELECT
    store_name,
    product_name,
    ROUND(
        total_quantity_sold / quantity_on_hand,
        2
    ) AS inventory_turnover_ratio
FROM gold_inventory_status
WHERE quantity_on_hand > 0
ORDER BY inventory_turnover_ratio DESC;



-- ==========================================================
-- KPI 5 : Top 3 Categories by Sales
-- ==========================================================

SELECT
    category,
    SUM(total_revenue) AS total_revenue
FROM gold_product_performance
GROUP BY
    category
ORDER BY
    total_revenue DESC
LIMIT 3;



-- ==========================================================
-- KPI 6 : Market Share
-- ==========================================================

SELECT
    store_name,
    market_share_percentage
FROM gold_sales_summary
ORDER BY
    market_share_percentage DESC;