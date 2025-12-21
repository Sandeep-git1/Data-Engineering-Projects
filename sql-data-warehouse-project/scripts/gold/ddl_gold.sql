---- Create Dimension Customer ----
-- we will create all the object and these objects will be virtual one, Hence we'll create a view

CREATE VIEW gold.dim_customers AS
 SELECT 
        ROW_NUMBER() OVER (ORDER BY cst_id) AS Customer_key,
        a.cst_id AS Customer_id,
        a.cst_key AS Customer_number,
        a.cst_firstname AS first_name,
        a.cst_lastname AS last_name,
        c.cntry AS country,
        a.cst_marital_status AS marital_status,
        CASE
            WHEN a.cst_gndr != 'n/a' then a.cst_gndr
            ELSE coalesce(b.gen, 'n/a')
        END AS gender,
        b.bdate AS birthday,
        a.cst_create_date AS create_date       
    FROM silver.crm_cust_info a
    LEFT JOIN silver.erp_cust_az12 b
        ON a.cst_key = b.cid
    LEFT JOIN silver.erp_loc_a101 c
        ON a.cst_key = c.cid

---- Create Dimension Products ----
CREATE VIEW gold.dim_product AS
 select 
     ROW_NUMBER() OVER (ORDER BY prd_start_dt, prd_key) AS product_key,
     a.prd_id AS product_id,
     a.prd_key AS product_number,
     a.prd_nm AS product_name,
     a.cat_id AS category_id,
     b.cat AS category,
     b.subcat AS subcategory,
     b.maintenance,
     a.prd_cost AS cost,
     a.prd_line AS product_line,
     a.prd_start_dt AS start_date
 from silver.crm_prd_info a
 LEFT JOIN silver.erp_px_cat_g1v2 b
 ON a.cat_id = b.id
 where prd_end_dt is null

 -- bcz this table has a decription, hence it is a dimension table so we will create a primary key for this 

  ---- Create Fact Sales ----
