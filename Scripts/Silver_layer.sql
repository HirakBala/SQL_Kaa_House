SELECT 'Creating and copying the data and tables from bronze layer';
use silver;
set @start_time = now();

-- Customer AZ Table
CREATE TABLE silver.silver_cust_az AS SELECT * FROM
    bronze.bronze_cust_az;

-- Customer Info Table
CREATE TABLE silver.silver_cust_info AS SELECT * FROM
    bronze.bronze_cust_info;

-- Location A Table
CREATE TABLE silver.silver_loc_a AS SELECT * FROM
    bronze.bronze_loc_a;

-- Product Category Table
CREATE TABLE silver.silver_prod_cat AS SELECT * FROM
    bronze.bronze_prod_cat;

-- Product Info Table
CREATE TABLE silver.silver_prod_info AS SELECT * FROM
    bronze.bronze_prod_info;

-- Sales Info Table
CREATE TABLE silver.silver_sales_info AS SELECT * FROM
    bronze.bronze_sales_info;

set @end_time = now();
set @total_time = timestampdiff(second, @start_time, @end_time);

SELECT 
    'Successfully ingested data from bronze' AS 'message',
    'Total time taken' AS 'message',
    @total_time AS 'message';

-- Time to apply transformations.

SELECT 
    cust_id, COUNT(cust_id) AS ' Total count'
FROM
    silver.silver_cust_info
GROUP BY cust_id
HAVING COUNT(cust_id) > 1;

-- Applying transformation,

SELECT 
    *
FROM
    silver.silver_cust_info
WHERE
    cust_id = 29433;
-- Will select the latest creation date of the customer. 

SELECT *
FROM (
    SELECT *, 
           ROW_NUMBER() OVER (PARTITION BY cust_id ORDER BY cust_createdate DESC) AS flag
    FROM silver_cust_info
) subquery
WHERE flag =1;

-- Completed

SELECT 
    cust_firstname
FROM
    silver.silver_cust_info
WHERE
    cust_firstname != TRIM(cust_firstname);
    
SELECT 
    cust_lastname
FROM
    silver.silver_cust_info
WHERE
    cust_lastname != TRIM(cust_lastname);  

set sql_safe_updates = 0;
-- Applying transformations,

UPDATE silver.silver_cust_info 
SET 
    cust_firstname = TRIM(cust_firstname)
WHERE
    cust_firstname IS NOT NULL
        AND cust_firstname != TRIM(cust_firstname);
  
UPDATE silver.silver_cust_info 
SET 
    cust_lastname = TRIM(cust_lastname)
WHERE
    cust_lastname IS NOT NULL
        AND cust_lastname != TRIM(cust_lastname);

-- Completed 

-- 3- Replace M with Males, F with Female and S with Single, M with Married in silver_cust_info table.  

alter table silver.silver_cust_info
modify column cust_gender varchar(10);

UPDATE silver.silver_cust_info 
SET 
    cust_gender = CASE
        WHEN cust_gender = 'F' THEN 'Female'
        WHEN cust_gender = 'M' THEN 'Male'
        ELSE cust_gender
    END;


UPDATE silver.silver_cust_info 
SET 
    cust_gender = CASE
        WHEN cust_gender = 'F' THEN 'Female'
        WHEN cust_gender = 'M' THEN 'Male'
        ELSE cust_gender
    END;
          
alter table silver.silver_cust_info
modify column cust_maritalstatus varchar(10);

UPDATE silver.silver_cust_info 
SET 
    cust_maritalstatus = CASE
        WHEN cust_maritalstatus = 'S' THEN 'Single'
        WHEN cust_maritalstatus = 'M' THEN 'Married'
        ELSE 'n/a'
    END;
          
-- Completed

-- 4- Check other columns and generate meaningful features.

SELECT 
    prd_id, COUNT(prd_id)
FROM
    silver.silver_prod_info
GROUP BY prd_id
HAVING COUNT(prd_id > 1);
-- Everything's safe.

SELECT 
    *
FROM
    silver.silver_prod_info;   

alter table silver.silver_prod_info
add column cat_id varchar(5);

UPDATE silver.silver_prod_info 
SET 
    cat_id = LEFT(prd_key, 5);

set sql_safe_updates = 0;

UPDATE silver.silver_prod_info 
SET 
    cat_id = LEFT(prd_key, 5);

SELECT 
    *
FROM
    silver.silver_prod_info;

-- In the silver_prod_info and silver_prod_cat we now have a common column through which we may join.
-- But, there's a difference in them which is an underscore.
-- Let's fix that.

UPDATE silver.silver_prod_info 
SET 
    cat_id = REPLACE(cat_id, '-', '_');

SELECT 
    *
FROM
    silver.silver_prod_info;

alter table silver.silver_prod_info
add column prd_key2 varchar(10);

UPDATE silver.silver_prod_info 
SET 
    prd_key2 = SUBSTRING(prd_key, 7, LENGTH(prd_key));

SELECT 
    *
FROM
    silver.silver_prod_info;

-- Completed

-- 5- Check the prd_cost column in silver_prod_info table.
 
SELECT 
    *
FROM
    silver.silver_prod_info
WHERE
    prd_cost IS NULL;

SELECT 
    *
FROM
    silver.silver_prod_info
WHERE
    prd_cost <= 0;

UPDATE silver.silver_prod_info 
SET 
    prd_cost = 0
WHERE
    prd_cost IS NULL;

-- Completed

-- 6- Trim and replace chars with meaningful words. 
 
SELECT 
    prd_line
FROM
    silver.silver_prod_info;

UPDATE silver.silver_prod_info 
SET 
    prd_line = CASE
        WHEN TRIM(prd_line) = 'M' THEN 'Mountain'
        WHEN TRIM(prd_line) = 'R' THEN 'Road'
        WHEN TRIM(prd_line) = 'S' THEN 'Other Sales'
        WHEN TRIM(prd_line) = 'T' THEN 'Touring'
        ELSE prd_line
    END;

SELECT 
    prd_line
FROM
    silver.silver_prod_info;

-- Completed

-- 7- Check for invalid date orders. 
 
SELECT 
    *
FROM
    silver.silver_prod_info
WHERE
    prd_startdate > prd_enddate;

-- Does'nt make any sense.

SELECT *
FROM (
    SELECT *,
           LEAD(prd_startdate) OVER (PARTITION BY prd_key ORDER BY prd_startdate) AS test
    FROM silver.silver_prod_info
) subquery
WHERE prd_startdate > test;

ALTER TABLE silver.silver_prod_info 
ADD COLUMN new_enddate DATE;

set sql_safe_updates = 0;
UPDATE silver.silver_prod_info spi
JOIN (
    SELECT 
        prd_key, 
        prd_startdate, 
        DATE_SUB(LEAD(prd_startdate) OVER (PARTITION BY prd_key ORDER BY prd_startdate), INTERVAL 1 DAY) AS calculated_enddate
    FROM silver.silver_prod_info
) subquery ON spi.prd_key = subquery.prd_key 
          AND spi.prd_startdate = subquery.prd_startdate
SET spi.new_enddate = subquery.calculated_enddate;

SELECT 
    *
FROM
    silver.silver_prod_info
WHERE
    prd_startdate > new_enddate;

alter table silver.silver_prod_info
drop column prd_enddate;

SELECT 
    *
FROM
    silver.silver_prod_info;
    
-- Completed 

-- 8- Check the dates in the silver_sales_info

select * from silver.silver_sales_info;

UPDATE silver.silver_sales_info
SET sls_orderdate = 
    CASE 
        WHEN sls_orderdate = 0 THEN NULL
        ELSE sls_orderdate
    END;

select sls_orderdate from silver.silver_sales_info
where length(sls_orderdate)<7;

DELETE FROM silver.silver_sales_info
WHERE LENGTH(sls_orderdate) < 7;

alter table silver.silver_sales_info
modify column sls_orderdate date;

alter table silver.silver_sales_info
modify column sls_orderdate date;

alter table silver.silver_sales_info
modify column sls_orderdate date;

select * from silver.silver_sales_info;

-- Completed

-- 9-   Check the other columns in silver_sales_info

SELECT 
    *
FROM
    silver.silver_sales_info
WHERE
    ROUND(COALESCE(sls_sales, 0), 2) <> ROUND(COALESCE(sls_price, 0) * COALESCE(sls_quantity, 0),
            2);

update silver.silver_sales_info
set sls_sales = case
when sls_sales <=0 or sls_sales IS NULL
then sls_price * sls_quantity
else sls_sales
end;

update silver.silver_sales_info
set sls_price = case
when sls_price <=0 or sls_price IS NULL
then sls_sales * sls_quantity
else sls_price
end;

update silver.silver_sales_info
set sls_price = case
when sls_price <0 
then abs(sls_price)
else sls_price
end;

SELECT 
    *
FROM
    silver.silver_sales_info
WHERE
    sls_price < 0 OR sls_price IS NULL
        OR sls_quantity < 0
        OR sls_quantity IS NULL
        OR sls_sales < 0
        OR sls_sales IS NULL;

SELECT 
    *
FROM
    silver.silver_sales_info;

-- Completed

-- 9- Check tha cid in silver_cust_az table.

UPDATE silver_cust_az 
SET 
    cid = REPLACE(cid, 'NAS', '');

set sql_safe_updates = 0 ;

UPDATE silver_cust_az 
SET 
    cid = REPLACE(cid, 'NAS', '');

SELECT 
    *
FROM
    silver.silver_cust_az;
    
-- Completed

-- 10- Check bdate in silver_cust_az table.

SELECT 
    bdate
FROM
    silver.silver_cust_az
WHERE
    bdate > CURDATE();

DELETE FROM silver.silver_cust_az 
WHERE
    bdate > CURDATE(); 

-- Completed

-- 11- Check the cid in silver_loc_a table.

SELECT 
    *
FROM
    silver.silver_loc_a;

set sql_safe_updates = 0 ;

UPDATE silver.silver_loc_a 
SET 
    cid = REPLACE(cid, '-', '');

SELECT 
    *
FROM
    silver.silver_loc_a;

-- Completed
 
-- 12- Check country in silver_loc_a table. 

SELECT DISTINCT
    (country)
FROM
    silver.silver_loc_a;

update silver.silver_loc_a set country = 
    CASE 
        WHEN country IN ('US', 'USA', 'United States') THEN 'USA' 
        ELSE country 
    END;

SELECT DISTINCT
    (country)
FROM
    silver.silver_loc_a;

DELETE FROM silver.silver_loc_a 
WHERE
    country IS NULL OR country = '';

SELECT DISTINCT
    (country)
FROM
    silver.silver_loc_a;

UPDATE silver.silver_loc_a 
SET 
    country = CASE
        WHEN country = 'DE' THEN 'Germany'
        ELSE country
    END;
    
SELECT DISTINCT
    (country)
FROM
    silver.silver_loc_a;

-- Completed

-- 13- Check the columns in silver_prod_cat table.

-- Everthing's fine no need to update or transform. 

-- Have transformed every table in the silver layer. Now it is time to load it into gold layer. 