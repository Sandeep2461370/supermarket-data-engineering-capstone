-- Inventory Optimization Analysis

-- ==========================================================
-- Q1. Which products are currently out of stock or low stock?
-- ==========================================================

SELECT
    store_name,
    product_name,
    quantity_on_hand,
    inventory_status
FROM gold_inventory_status
WHERE inventory_status IN ('Out of Stock', 'Low Stock')
ORDER BY
    quantity_on_hand,
    store_name;


-- ==========================================================
-- Q2. Which products have the highest sales velocity?
-- ==========================================================

SELECT
    store_name,
    product_name,
    total_quantity_sold,
    sales_velocity
FROM gold_inventory_status
ORDER BY
    sales_velocity DESC;


-- ==========================================================
-- Q3. Which products are slow-moving and overstock candidates?
-- ==========================================================

SELECT
    store_name,
    product_name,
    quantity_on_hand,
    total_quantity_sold,
    sales_velocity,
    reorder_point
FROM gold_inventory_status
WHERE
    quantity_on_hand > reorder_point
    AND sales_velocity < 1
ORDER BY
    quantity_on_hand DESC;


-- ==========================================================
-- Q4. What is the average days to deliver for each supplier?
-- ==========================================================

SELECT
    supplier_name,
    average_delivery_days
FROM gold_supplier_performance
ORDER BY
    average_delivery_days;


-- ==========================================================
-- Q5. Which suppliers are most reliable?
-- ==========================================================

SELECT
    supplier_name,
    total_orders,
    delivered_orders,
    cancelled_orders,
    delivery_completion_rate
FROM gold_supplier_performance
ORDER BY
    delivery_completion_rate DESC;


-- ==========================================================
-- Q6. What is the optimal reorder point for each product-store combination?
-- ==========================================================

SELECT
    store_name,
    product_name,
    quantity_on_hand,
    sales_velocity,
    reorder_point
FROM gold_inventory_status
ORDER BY
    store_name,
    product_name;