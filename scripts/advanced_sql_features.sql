-- Computes advanced metrics and derived features in the warehouse

-- Rolling 3-month sales per customer
DROP TABLE IF EXISTS customer_rolling_sales;
CREATE TABLE customer_rolling_sales AS
WITH customer_orders AS (
    SELECT
        customer_key,
        date_id,
        SUM(sales) AS daily_sales
    FROM fact_sales
    GROUP BY customer_key, date_id
)
SELECT
    customer_key,
    date_id,
    SUM(daily_sales) OVER (
        PARTITION BY customer_key
        ORDER BY date_id
        ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
    ) AS rolling_3mo_sales
FROM customer_orders;

-- Customer Lifetime Value (CLV)
DROP TABLE IF EXISTS customer_lifetime_value;
CREATE TABLE customer_lifetime_value AS
SELECT
    customer_key,
    SUM(sales - shipping_cost) AS clv
FROM fact_sales
GROUP BY customer_key;
