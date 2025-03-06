select "Creating the schema for the data warehouse" as message;
set @start_time = now();
create schema bronze;
create schema silver;
create schema gold;
set @end_time = now();
select "Creation of the schema completed" as message;
set @total_time = timestampdiff(second , @start_time, @end_time);
select @total_time as "Total time creation of schemas in seconds";
use bronze;

select "Creating tables for bronze layer" as message;
set @start_time = now();
CREATE TABLE cust_info (
    cust_id INT,
    cust_key VARCHAR(70),
    cust_firstname VARCHAR(50),
    cust_lastname VARCHAR(50),
    cust_martialstatus VARCHAR(1),
    cust_gender VARCHAR(1),
    cust_createdate DATE
);
CREATE TABLE products (
    prd_id INT,
    prd_key VARCHAR(255),
    prd_nm VARCHAR(255),
    prd_cost INT,
    prd_line VARCHAR(255),
    prd_startdate DATE,
    prd_enddate DATE
);
CREATE TABLE sales (
    sls_ordnum VARCHAR(50),
    sls_prdkey VARCHAR(50),
    sls_custid INT,
    sls_orderdate INT,
    sls_shipdate INT,
    sls_duedate INT,
    sls_sales INT,
    sls_quantity INT,
    sls_price INT
);
CREATE TABLE cust_az (
    cid VARCHAR(50),
    bdate DATE,
    gen VARCHAR(1)
);
CREATE TABLE loc_a (
    cid VARCHAR(50),
    country VARCHAR(50)
);
CREATE TABLE prod_cat (
    id VARCHAR(50),
    cat VARCHAR(50),
    subcat VARCHAR(50),
    maintenance VARCHAR(50)
);
set @end_time = now();
set @total_time= timestampdiff(second , @start_time, @end_time);
select @total_time as "Total time creation of tables in seconds";


select "Doing some adjustments" as message;
alter table cust_info
rename column cust_martialstatus to cust_maritalstatus;

ALTER TABLE cust_az
MODIFY COLUMN gen VARCHAR(10);

alter table products rename to prod_info;

alter table sales rename to sales_info;

alter table sales_info
modify column sls_shipdate date;

alter table sales_info
modify column sls_duedate date;

alter table cust_az
rename to bronze_cust_az;

alter table cust_info
rename to bronze_cust_info;

alter table loc_a
rename to bronze_loc_a;

alter table prod_cat
rename to bronze_prod_cat;

alter table prod_info
rename to bronze_prod_info;

alter table sales_info
rename to bronze_sales_info;
select "Adjustments completed" as message;

-- Now, run the python code to ingest data into bronze layer. 