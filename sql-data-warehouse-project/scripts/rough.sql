---- Create Dimension Customers ----
SELECT * from [silver].[crm_cust_info]
SELECT * from silver.erp_cust_az12
SELECT * from silver.erp_loc_a101


SELECT 
    a.cst_id,
    a.cst_key,
    a.cst_firstname,
    a.cst_lastname,
    a.cst_marital_status,
    a.cst_gndr,
    a.cst_create_date,
    b.bdate,
    b.gen,
    c.cntry
FROM silver.crm_cust_info a
LEFT JOIN silver.erp_cust_az12 b
    ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 c
    ON a.cst_key = c.cid;

-- checking duplicate data --
SELECT 
    cst_id,
    COUNT(*) 
FROM (
    SELECT 
        a.cst_id,
        a.cst_key,
        a.cst_firstname,
        a.cst_lastname,
        a.cst_marital_status,
        a.cst_gndr,
        a.cst_create_date,
        b.bdate,
        b.gen,
        c.cntry
    FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Data Intergartion Problem --
select
a.cst_gndr,
b.gen
FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid
-- Updating the info with assuming the master table as the crm_cust_info e.g. all the info here is legit then 
-- consider the second one

select
a.cst_gndr,
CASE
    WHEN a.cst_gndr != 'n/a' then a.cst_gndr
    ELSE coalesce(b.gen, 'n/a')
END AS gender,
b.gen
FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid

-- checking quality --
 select distinct gender from gold.dim_customers


 ----- Create Dimension Products -----

 select * from silver.crm_prd_info
 select * from silver.erp_px_cat_g1v2

 select 
     a.prd_id,
     a.cat_id,
     a.prd_key,
     a.prd_nm,
     a.prd_cost,
     a.prd_line,
     a.prd_start_dt,
     b.cat,
     b.subcat,
     b.maintenance
 from silver.crm_prd_info a
 LEFT JOIN silver.erp_px_cat_g1v2 b
 ON a.cat_id = b.id
 where prd_end_dt is null  --filter out all the historical data

 -- check the uniqueness --
 select prd_key, count(*) from (
  select 
     a.prd_id,
     a.cat_id,
     a.prd_key,
     a.prd_nm,
     a.prd_cost,
     a.prd_line,
     a.prd_start_dt,
     b.cat,
     b.subcat,
     b.maintenance
 from silver.crm_prd_info a
 LEFT JOIN silver.erp_px_cat_g1v2 b
 ON a.cat_id = b.id
 where prd_end_dt is null
 ) t
 group by prd_key
 having count(*) > 1


 -- checking quality --
 select * from gold.dim_product


 ---- Create Fact Sales ----
 CREATE VIEW gold.dim_fact_sales AS
 SELECT 
    -- dimesion keys
     a.sls_ord_num AS order_number ,
     b.product_key,
     c.Customer_key,
     --Dates
     a.sls_order_dt AS order_date,
     a.sls_ship_dt AS ship_date,
     a.sls_due_dt AS due_date,
     --measures
     a.sls_sales AS sales,
     a.sls_quantity AS quantity,
     a.sls_price AS price
from silver.crm_sales_details a
LEFT JOIN gold.dim_product b
ON a.sls_prd_key = b.product_number
LEFT JOIN gold.dim_customers c
ON a.sls_cust_id = c.Customer_id

select * 
from gold.dim_fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
where c.customer_key is null -- we are not getting anything in the result, hence it means it matches everything. check for the product table as well.


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

---- Create Dimension Customers ----
SELECT * from [silver].[crm_cust_info]
SELECT * from silver.erp_cust_az12
SELECT * from silver.erp_loc_a101


SELECT 
    a.cst_id,
    a.cst_key,
    a.cst_firstname,
    a.cst_lastname,
    a.cst_marital_status,
    a.cst_gndr,
    a.cst_create_date,
    b.bdate,
    b.gen,
    c.cntry
FROM silver.crm_cust_info a
LEFT JOIN silver.erp_cust_az12 b
    ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 c
    ON a.cst_key = c.cid;

-- checking duplicate data --
SELECT 
    cst_id,
    COUNT(*) 
FROM (
    SELECT 
        a.cst_id,
        a.cst_key,
        a.cst_firstname,
        a.cst_lastname,
        a.cst_marital_status,
        a.cst_gndr,
        a.cst_create_date,
        b.bdate,
        b.gen,
        c.cntry
    FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid
) t
GROUP BY cst_id
HAVING COUNT(*) > 1;

-- Data Intergartion Problem --
select
a.cst_gndr,
b.gen
FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid
-- Updating the info with assuming the master table as the crm_cust_info e.g. all the info here is legit then 
-- consider the second one

select
a.cst_gndr,
CASE
    WHEN a.cst_gndr != 'n/a' then a.cst_gndr
    ELSE coalesce(b.gen, 'n/a')
END AS gender,
b.gen
FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid

-- checking quality --
 select distinct gender from gold.dim_customers


 ----- Create Dimension Products -----

 select * from silver.crm_prd_info
 select * from silver.erp_px_cat_g1v2

 select 
     a.prd_id,
     a.cat_id,
     a.prd_key,
     a.prd_nm,
     a.prd_cost,
     a.prd_line,
     a.prd_start_dt,
     b.cat,
     b.subcat,
     b.maintenance
 from silver.crm_prd_info a
 LEFT JOIN silver.erp_px_cat_g1v2 b
 ON a.cat_id = b.id
 where prd_end_dt is null  --filter out all the historical data

 -- check the uniqueness --
 select prd_key, count(*) from (
  select 
     a.prd_id,
     a.cat_id,
     a.prd_key,
     a.prd_nm,
     a.prd_cost,
     a.prd_line,
     a.prd_start_dt,
     b.cat,
     b.subcat,
     b.maintenance
 from silver.crm_prd_info a
 LEFT JOIN silver.erp_px_cat_g1v2 b
 ON a.cat_id = b.id
 where prd_end_dt is null
 ) t
 group by prd_key
 having count(*) > 1


 -- checking quality --
 select * from gold.dim_product


 ---- Create Fact Sales ----
 CREATE VIEW gold.dim_fact_sales AS
 SELECT 
    -- dimesion keys
     a.sls_ord_num AS order_number ,
     b.product_key,
     c.Customer_key,
     --Dates
     a.sls_order_dt AS order_date,
     a.sls_ship_dt AS ship_date,
     a.sls_due_dt AS due_date,
     --measures
     a.sls_sales AS sales,
     a.sls_quantity AS quantity,
     a.sls_price AS price
from silver.crm_sales_details a
LEFT JOIN gold.dim_product b
ON a.sls_prd_key = b.product_number
LEFT JOIN gold.dim_customers c
ON a.sls_cust_id = c.Customer_id

select * 
from gold.dim_fact_sales f
LEFT JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
where c.customer_key is null -- we are not getting anything in the result, hence it means it matches everything. check for the product table as well.

----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

EXEC silver.load_silver
CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
    --cust_info
    PRINT'>> Truncating Table: silver.crm_cust_info'
    TRUNCATE TABLE silver.crm_cust_info;
    PRINT '>> Inserting Data Into: silver.crm_cust_info'
    INSERT INTO silver.crm_cust_info (
        cst_id,
        cst_key,
        cst_firstname,
        cst_lastname,
        cst_marital_status,
        cst_gndr,
        cst_create_date
    )
    SELECT
        cst_id,
        cst_key,
        TRIM(cst_firstname) AS cst_firstname,
        TRIM(cst_lastname)  AS cst_lastname,

        CASE
            WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
            WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
            ELSE 'n/a'
        END AS cst_marital_status,

        CASE
            WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
            WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
            ELSE 'n/a'
        END AS cst_gndr,

        cst_create_date
    FROM (
        SELECT
            *,
            ROW_NUMBER() OVER (
                PARTITION BY cst_id
                ORDER BY cst_create_date DESC
            ) AS rank_no
        FROM bronze.crm_cust_info
        WHERE cst_id IS NOT NULL
    ) t
    WHERE rank_no = 1;


    --prd_info
    PRINT'>> Truncating Table: silver.crm_prd_info'
    TRUNCATE TABLE silver.crm_prd_info;
    PRINT '>> Inserting Data Into: silver.crm_prd_info'
    INSERT INTO silver.crm_prd_info (
        prd_id,
        cat_id,
        prd_key,
        prd_nm,
        prd_cost,
        prd_line,
        prd_start_dt,
        prd_end_dt
    )
    SELECT 
        prd_id,
        REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
        SUBSTRING(prd_key, 7, LEN(prd_key))         AS prd_key,
        prd_nm,
        ISNULL(prd_cost, 0)                         AS prd_cost,

        CASE UPPER(TRIM(prd_line))
            WHEN 'M' THEN 'Mountain'
            WHEN 'R' THEN 'Road'
            WHEN 'S' THEN 'Other Sales'
            WHEN 'T' THEN 'Touring'
            ELSE 'n/a'
        END AS prd_line,

        CAST(prd_start_dt AS DATE) AS prd_start_dt,

        CAST(
            LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key
                ORDER BY prd_start_dt
            ) - 1
            AS DATE
        ) AS prd_end_dt
    FROM bronze.crm_prd_info;


    -- sales_detail
    PRINT'>> Truncating Table: silver.crm_sales_details'
    TRUNCATE TABLE silver.crm_sales_details;
    PRINT '>> Inserting Data Into: silver.crm_sales_details'
    INSERT INTO silver.crm_sales_details (
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,
        sls_order_dt,
        sls_ship_dt,
        sls_due_dt,
        sls_sales,
        sls_quantity,
        sls_price
    )
    SELECT
        sls_ord_num,
        sls_prd_key,
        sls_cust_id,

        CASE
            WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_order_dt AS VARCHAR(8)) AS DATE)
        END AS sls_order_dt,

        CASE
            WHEN sls_ship_dt <= 0 OR LEN(sls_ship_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_ship_dt AS VARCHAR(8)) AS DATE)
        END AS sls_ship_dt,

        CASE
            WHEN sls_due_dt <= 0 OR LEN(sls_due_dt) <> 8 THEN NULL
            ELSE CAST(CAST(sls_due_dt AS VARCHAR(8)) AS DATE)
        END AS sls_due_dt,

        CASE
            WHEN sls_sales <= 0
              OR sls_sales IS NULL
              OR sls_sales <> sls_quantity * ABS(sls_price)
            THEN sls_quantity * ABS(sls_price)
            ELSE sls_sales
        END AS sls_sales,

        sls_quantity,

        CASE
            WHEN sls_price <= 0 OR sls_price IS NULL
            THEN sls_sales / NULLIF(sls_quantity, 0)
            ELSE sls_price
        END AS sls_price
    FROM bronze.crm_sales_details;


    -- erp1
    PRINT'>> Truncating Table: silver.erp_cust_az12'
    TRUNCATE TABLE silver.erp_cust_az12;
    PRINT '>> Inserting Data Into: silver.erp_cust_az12'
    INSERT INTO silver.erp_cust_az12 (
        cid,
        bdate,
        gen
    )
    SELECT 
        CASE
            WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
            ELSE cid
        END AS cid,

        CASE 
            WHEN bdate > GETDATE() THEN NULL
            ELSE bdate
        END AS bdate,

        CASE
            WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
            WHEN UPPER(TRIM(gen)) IN ('M', 'MALE')   THEN 'Male'
            ELSE 'n/a'
        END AS gen
    FROM bronze.erp_cust_az12;


    --erp_loc 2
    PRINT'>> Truncating Table: silver.erp_loc_a101'
    TRUNCATE TABLE silver.erp_loc_a101;
    PRINT '>> Inserting Data Into: silver.erp_loc_a101'
    INSERT INTO silver.erp_loc_a101 (
        cid,
        cntry
    )
    SELECT 
        REPLACE(cid, '-', '') AS cid,

        CASE
            WHEN TRIM(cntry) = 'DE' THEN 'Germany'
            WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
            WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
            ELSE cntry
        END AS cntry
    FROM bronze.erp_loc_a101;


    -- erp_px_cat 3
    PRINT'>> Truncating Table: silver.erp_px_cat_g1v2'
    TRUNCATE TABLE silver.erp_px_cat_g1v2;
    PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2'
    INSERT INTO silver.erp_px_cat_g1v2 (
        id,
        cat,
        subcat,
        maintenance
    )
    SELECT
        id,
        cat,
        subcat,
        maintenance
    FROM bronze.erp_px_cat_g1v2;
END
