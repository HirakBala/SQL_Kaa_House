-- Database Exploration 

SELECT 
    *
FROM
    information_schema.tables;
SELECT 
    *
FROM
    information_schema.columns
WHERE
    table_name = 'dimension_customers';

-- Dimension Exploration

SELECT DISTINCT
    (country)
FROM
    gold.dimension_customers;
SELECT DISTINCT
    (product_category)
FROM
    gold.dimension_products;
SELECT DISTINCT
    (product_subcategory)
FROM
    gold.dimension_products;

-- Date Exploration

SELECT 
    MIN(order_date) AS first_orderdate,
    MAX(order_date) AS last_orderdate,
    TIMESTAMPDIFF(YEAR,
        MIN(order_date),
        MAX(order_date)) AS date_difference
FROM
    gold.fact_sales;

SELECT 
    MIN(birthday_date) AS oldest, MAX(birthday_date) AS youngest
FROM
    gold.dimension_customers;

-- Measure Exploration

-- 1- Total Sales  
SELECT 
    SUM(sales_amount) AS total_sales
FROM
    gold.fact_sales;
    
-- 2-  Total itmes sold
SELECT 
    COUNT(quantity) AS items_sold
FROM
    gold.fact_sales;
    
-- 3- Avg selling price
SELECT 
    AVG(sales_amount) AS avg_sell_price
FROM
    gold.fact_sales;
    
-- 4- Total orders
SELECT 
    COUNT(DISTINCT order_number) AS total_orders
FROM
    gold.fact_sales;
    
-- 5- Total products
SELECT 
    COUNT(DISTINCT product_key) AS total_products
FROM
    gold.dimension_products;
    
-- 6- Total Customers
SELECT 
    COUNT(customer_key) AS total_customer
FROM
    gold.dimension_customers; 

-- Magnitude

-- 1- Total customers by country
SELECT 
    country, COUNT(customer_key) AS total_customers
FROM
    gold.dimension_customers
WHERE
    country IS NOT NULL
GROUP BY country;

-- 2- Total customers by gender
SELECT 
    gender, COUNT(customer_key) AS total_customers
FROM
    gold.dimension_customers
WHERE
    gender IS NOT NULL
GROUP BY gender;

-- 3- Total products by category
SELECT 
    product_category, COUNT(product_key) AS total_products
FROM
    gold.dimension_products
GROUP BY product_category; 

-- 4- Avg cost in each category 
SELECT 
    product_category, AVG(product_cost) AS avg_cost
FROM
    gold.dimension_products
GROUP BY product_category; 

-- 5- Total reveunue by each category
SELECT 
    dc.product_category, SUM(fc.sales_amount) AS total_revenue
FROM
    gold.fact_sales fc
        LEFT JOIN
    gold.dimension_products dc ON dc.product_key = fc.product_key
GROUP BY dc.product_category;

-- Ranking

-- 1- Top 5 products that generated highest revenue
SELECT 
    dc.product_name, SUM(fc.sales_amount) AS total_sales
FROM
    gold.fact_sales fc
        LEFT JOIN
    gold.dimension_products dc ON fc.product_key = dc.product_key
GROUP BY dc.product_name
ORDER BY total_sales DESC
LIMIT 5;

-- 2- Top 5 worst products in terms of sales
SELECT 
    dc.product_name, SUM(fc.sales_amount) AS total_sales
FROM
    gold.fact_sales fc
        LEFT JOIN
    gold.dimension_products dc ON fc.product_key = dc.product_key
GROUP BY dc.product_name
ORDER BY total_sales
LIMIT 5;

-- Changes over time
SELECT 
    YEAR(order_date) AS year,
    MONTH(order_date) AS month,
    SUM(sales_amount) AS sales,
    COUNT(DISTINCT customer_key) AS customers,
    COUNT(quantity) AS quantity
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY YEAR(order_date) , MONTH(order_date);

-- Cummulative analysis
select year, month, sales,  
sum(sales) over(partition by year order  by month) as running_total from(
SELECT 
	year(order_date) as year,
    MONTH(order_date) AS month,
    SUM(sales_amount) AS sales
FROM
    gold.fact_sales
WHERE
    order_date IS NOT NULL
GROUP BY  month(order_date) , year(order_date)
order by year(order_date)) subquery;

-- Performance analysis
-- Compare sales with previous year sales 
WITH yearly_sales AS (
    SELECT 
        YEAR(fc.order_date) AS year, 
        dc.product_name AS product_name, 
        SUM(fc.sales_amount) AS total_sales 
    FROM gold.fact_sales fc 
    LEFT JOIN gold.dimension_products dc ON fc.product_key = dc.product_key
    GROUP BY YEAR(fc.order_date), dc.product_name
)SELECT 
    year, 
    product_name, 
    total_sales,
    LAG(total_sales) OVER (PARTITION BY product_name ORDER BY year) AS previous_year_sales
FROM yearly_sales
ORDER BY product_name, year;

-- Part to whole analysis
-- Which category contributes more to the overall sales
with category_sales as (
select dc.product_category as products, sum(fc.sales_amount) as total_sales from gold.fact_sales fc 
left join gold.dimension_products dc on  fc.product_key = dc.product_key
group by product_category)

select products, total_sales , sum(total_Sales) over() overall_sales, round((cast(total_sales as float)/ sum(total_sales) over()) * 100,2)
as percentage_of_total from category_sales ;
 
-- Analysis completed