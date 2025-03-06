use gold;
create or replace view gold.dimension_customers as 
SELECT 
	row_number() over(order by cust_id) as customer_key,
    ci.cust_id AS customer_id,
    ci.cust_key AS customer_number,
    ci.cust_firstname AS firstname,
    ci.cust_lastname AS lastname,
    ci.cust_gender AS gender,
    loc.country AS country,
    ci.cust_maritalstatus AS marital_status,
    cz.bdate AS birthday_date
FROM
    silver.silver_cust_info ci
        LEFT JOIN
    silver.silver_cust_az cz ON ci.cust_key = cz.cid
        LEFT JOIN
    silver.silver_loc_a loc ON ci.cust_key = loc.cid;

create or replace view gold.dimension_products as 
SELECT 
	row_number() over (order by pi.prd_startdate, pi.prd_key2) as product_key,
    pi.prd_id as product_id,
    pi.prd_key2 as product_number,
	pi.prd_nm as product_name,
    pi.cat_id as category_id,
    pc.cat as product_category,
    pc.subcat as product_subcategory,
    pc.maintenance,
    pi.prd_cost as product_cost,
    pi.prd_line as product_line,
    pi.prd_startdate as product_startdate
FROM
    silver.silver_prod_info pi
        LEFT JOIN
    silver.silver_prod_cat pc ON pi.cat_id = pc.id
WHERE
    pi.new_enddate IS NULL;
    
CREATE OR REPLACE VIEW gold.fact_sales AS
    SELECT 
        si.sls_ordnum AS order_number,
        dc.customer_key,
        pr.product_key,
        si.sls_orderdate AS order_date,
        si.sls_shipdate AS ship_date,
        si.sls_duedate AS due_date,
        si.sls_sales AS sales_amount,
        si.sls_quantity AS quantity,
        si.sls_price AS unit_price
    FROM
        silver.silver_sales_info si
            LEFT JOIN
        gold.dimension_products pr ON TRIM(si.sls_prdkey) = TRIM(pr.product_number)
            LEFT JOIN
        gold.dimension_customers dc ON TRIM(si.sls_custid) = TRIM(dc.customer_id);

-- Completed the gold layer. Finished the data architecture. 