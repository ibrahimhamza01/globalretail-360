------------------------------
-- Create dim_date
------------------------------
DROP TABLE IF EXISTS dim_date;

CREATE TABLE dim_date AS
WITH date_range AS (
    SELECT generate_series(
        '2014-01-01'::date,
        '2017-12-31'::date,
        '1 day'::interval
    ) AS order_date
)
SELECT
    TO_CHAR(order_date, 'YYYYMMDD')::int AS date_id,
    order_date,
    EXTRACT(YEAR FROM order_date) AS year,
    EXTRACT(QUARTER FROM order_date) AS quarter,
    EXTRACT(MONTH FROM order_date) AS month,
    TO_CHAR(order_date, 'Month') AS month_name,
    EXTRACT(DAY FROM order_date) AS day,
    TO_CHAR(order_date, 'Day') AS weekday,
    CASE WHEN EXTRACT(DOW FROM order_date) IN (0,6) THEN 1 ELSE 0 END AS is_weekend
FROM date_range
ORDER BY order_date;

SELECT * FROM dim_date LIMIT 10;

SELECT MIN(order_date), MAX(order_date) FROM dim_date;

------------------------------
-- Create dim_customers
------------------------------
DROP TABLE IF EXISTS dim_customers;

CREATE TABLE dim_customers AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "Customer ID") AS customer_key,  -- surrogate key
    "Customer ID" AS customer_id,
    "Customer Name" AS customer_name,
    "Segment" AS segment,
    "City" AS city,
    "State" AS state,
    "Region" AS region,
    "Postal Code" AS postal_code,
    "Country" AS country
FROM customers
ORDER BY "Customer ID";

SELECT * FROM dim_customers LIMIT 10;

SELECT COUNT(*) FROM dim_customers;

SELECT MIN(customer_key), MAX(customer_key) FROM dim_customers;

------------------------------
-- Create dim_products (internal)
------------------------------
DROP TABLE IF EXISTS dim_products;

CREATE TABLE dim_products AS
SELECT
    ROW_NUMBER() OVER (ORDER BY "Product ID") AS product_key,  -- surrogate key
    "Product ID" AS product_id,
    "Product Name" AS product_name,
    "Category" AS category,
    "Sub-Category" AS sub_category,
    NULL::numeric AS avg_internal_price  -- placeholder to compute later from orders
FROM orders
GROUP BY "Product ID", "Product Name", "Category", "Sub-Category"
ORDER BY "Product ID";

UPDATE dim_products dp
SET avg_internal_price = sub.avg_price
FROM (
    SELECT 
        "Product ID" AS product_id,
        SUM("Sales") / SUM("Quantity") AS avg_price
    FROM orders
    GROUP BY "Product ID"
) AS sub
WHERE dp.product_id = sub.product_id;

SELECT * FROM dim_products LIMIT 10;

SELECT COUNT(*) FROM dim_products;

SELECT MIN(avg_internal_price), MAX(avg_internal_price) FROM dim_products;

------------------------------
-- Create dim_fake_products (synthetic / enrichment)
------------------------------
DROP TABLE IF EXISTS dim_fake_products;

CREATE TABLE dim_fake_products AS
SELECT 
    id AS fake_product_id,
    title AS product_name,
    category,
    price,
    description,
    image,
    rating_rate,
    rating_count
FROM fake_store_products;

SELECT * FROM dim_fake_products LIMIT 10;

SELECT COUNT(*) FROM dim_fake_products;

SELECT MIN(price), MAX(price) FROM dim_fake_products;

------------------------------
-- Create dim_geography
------------------------------
DROP TABLE IF EXISTS dim_geography;

CREATE TABLE dim_geography AS
SELECT
    ROW_NUMBER() OVER (ORDER BY customers."Country", customers."Region", customers."State", customers."City") AS geography_key,  -- surrogate key
    customers."City" AS city,
    customers."State" AS state,
    customers."Region" AS region,
    customers."Country" AS country
FROM customers
GROUP BY customers."City", customers."State", customers."Region", customers."Country"
ORDER BY customers."Country", customers."Region", customers."State", customers."City";

SELECT * FROM dim_geography LIMIT 10;
SELECT COUNT(*) FROM dim_geography;
SELECT MIN(geography_key), MAX(geography_key) FROM dim_geography;

------------------------------
-- Create fact_sales
------------------------------
DROP TABLE IF EXISTS fact_sales;

CREATE TABLE fact_sales AS
SELECT
    o."Order ID" AS order_id,
    dc.customer_key,
    dp.product_key,
    dd.date_id,
    dg.geography_key,
    o."Sales" AS sales,
    o."Profit" AS profit,
    o."Discount" AS discount,
    o."Quantity" AS quantity,
    o."Shipping Cost" AS shipping_cost,
    CASE WHEN r."Returned" = 'Yes' THEN 1 ELSE 0 END AS return_flag,
    CASE WHEN o."Profit" < 0 THEN 1 ELSE 0 END AS loss_flag
FROM orders o
-- Link to date dimension
JOIN dim_date dd
    ON o."Order Date"::date = dd.order_date
-- Link to customer dimension
JOIN dim_customers dc
    ON o."Customer ID" = dc.customer_id
-- Link to product dimension
JOIN dim_products dp
    ON o."Product ID" = dp.product_id
-- Link to geography dimension via customer
JOIN dim_geography dg
    ON dc.city = dg.city
    AND dc.state = dg.state
    AND dc.region = dg.region
    AND dc.country = dg.country
-- Left join returns for return_flag
LEFT JOIN returns r
    ON o."Order ID" = r."Order ID";

select * from fact_sales limit 10;


----

-- Add primary keys to dimension tables
ALTER TABLE dim_date
ADD PRIMARY KEY (date_id);

ALTER TABLE dim_customers
ADD PRIMARY KEY (customer_key);

ALTER TABLE dim_products
ADD PRIMARY KEY (product_key);

ALTER TABLE dim_geography
ADD PRIMARY KEY (geography_key);

-- Add foreign keys from fact_sales to dimensions
ALTER TABLE fact_sales
ADD CONSTRAINT fk_fact_date
FOREIGN KEY (date_id) REFERENCES dim_date(date_id);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_fact_customer
FOREIGN KEY (customer_key) REFERENCES dim_customers(customer_key);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_fact_product
FOREIGN KEY (product_key) REFERENCES dim_products(product_key);

ALTER TABLE fact_sales
ADD CONSTRAINT fk_fact_geography
FOREIGN KEY (geography_key) REFERENCES dim_geography(geography_key);

SELECT table_schema, table_name
FROM information_schema.tables
WHERE table_name LIKE 'dim_%' OR table_name LIKE 'fact_%';