CREATE OR ALTER VIEW gold.dim_customers AS
SELECT
    a.cst_id       AS customer_id,
    a.cst_key,
    a.cst_firstname,
    a.cst_lastname,
    a.cst_marital_status,
    CASE
        WHEN a.cst_gndr <> 'n/a' THEN a.cst_gndr
        ELSE COALESCE(b.gen, 'n/a')
    END AS gender,
    b.bdate,
    c.cntry,
    a.cst_create_date
FROM silver.crm_cust_info a
LEFT JOIN silver.erp_cust_az12 b
    ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 c
    ON a.cst_key = c.cid;
-----------------------------------------------------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.dim_product AS
SELECT
    a.prd_id,
    a.prd_key          AS product_number,
    a.prd_nm,
    a.prd_cost,
    a.prd_line,
    b.cat,
    b.subcat,
    b.maintenance,
    a.prd_start_dt
FROM silver.crm_prd_info a
LEFT JOIN silver.erp_px_cat_g1v2 b
    ON a.cat_id = b.id
WHERE a.prd_end_dt IS NULL;
------------------------------------------------------------------------------------------------------------------------

CREATE OR ALTER VIEW gold.fact_sales AS
SELECT
    a.sls_ord_num      AS order_number,
    b.product_key,
    c.customer_key,
    a.sls_order_dt     AS order_date,
    a.sls_ship_dt      AS ship_date,
    a.sls_due_dt       AS due_date,
    a.sls_sales        AS sales,
    a.sls_quantity     AS quantity,
    a.sls_price        AS price
FROM silver.crm_sales_details a
LEFT JOIN gold.dim_product b
    ON a.sls_prd_key = b.product_number
LEFT JOIN gold.dim_customers c
    ON a.sls_cust_id = c.customer_id;
------------------------------------------------------------------------------------------------------------------------
