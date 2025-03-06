use gold;
create or replace view  product_report as (WITH base_query AS (
    SELECT dp.product_name, 
           dp.product_category, 
           dp.product_subcategory, 
           fc.sales_amount, 
           fc.quantity, 
           fc.unit_price 
    FROM gold.fact_sales fc 
    LEFT JOIN gold.dimension_products dp 
    ON fc.product_key = dp.product_key
),
segments AS (
    SELECT product_name, 
           product_category, 
           product_subcategory, 
           sales_amount, 
           quantity, 
           unit_price, 
           CASE 
               WHEN sales_amount = 0 THEN 0 
               ELSE sales_amount * quantity 
           END AS revenue 
    FROM base_query
) 
SELECT product_name, 
       product_category, 
       product_subcategory, 
       sales_amount, 
       quantity, 
       unit_price, 
       revenue ,
       case when revenue between 0 and 1500 then "Low Performers"  when revenue between 1501 and 3000 then "Mid Performers" 
       when revenue >3000 then "High Performers" end as revenue_groups
FROM segments);
