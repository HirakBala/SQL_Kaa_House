use gold;
create or replace view customer_report as(
With base_query as (SELECT 
    fc.order_number,
    fc.product_key,
    fc.order_date,
    fc.sales_amount,
    fc.quantity,
    dc.customer_key,
    dc.customer_number,
    CONCAT(dc.firstname, ' ', dc.lastname) AS customer_name,
    TIMESTAMPDIFF(YEAR,
        dc.birthday_date,
        CURDATE()) AS age
FROM
    gold.fact_sales fc
        LEFT JOIN
    gold.dimension_customers dc ON fc.customer_key = dc.customer_key
WHERE
    fc.order_date IS NOT NULL) 
    , customer_agg as (
    select customer_key,
    customer_number,
    customer_name,
    age , count(distinct order_number) as total_orders , sum(sales_amount) as total_sales, sum(quantity) as total_quantity, count(distinct product_key) as total_products from base_query group by customer_key,
    customer_number,
    customer_name,
    age) select 
    customer_key,
    customer_number,
    customer_name,
    age,
    case when age < 20 then "Under 20"
			when age between 20 and 29 then "20-29"
			when age between 30 and 39 then "30-39"
            when age between 40 and 49 then "40-49" 
            else "Above 50"
            end as age_group,
    total_orders , total_sales,total_quantity, total_products, 
    case when total_sales = 0 then "0"  else total_sales/ total_orders end as avg_order_value  from customer_agg);