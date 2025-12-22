/*
This module defines the Gold layer of the data warehouse, delivering business-ready dimension and fact views built on top of the Silver layer.

# Schema Design
Implements a Star Schema for analytics and reporting
Uses surrogate keys for all dimensions
Ensures clean, enriched, and conformed data

# Objects Created
  gold.dim_customers
      Customer master dimension
      Enriched with demographic and location data
      Handles gender fallback logic across source systems

  gold.dim_product
      Product master dimension
      Includes category, subcategory, and maintenance attributes
      Filters out historical (inactive) products

  gold.fact_sales
      Sales fact view
      Links to customer and product dimensions via surrogate keys
      Contains measures such as sales amount, quantity, and price
*/


/*==============================================================================
Create Dimension: gold.dim_customers
==============================================================================*/
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO

CREATE VIEW gold.dim_customers AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY a.cst_id) AS customer_key,   -- Surrogate Key
    a.cst_id            AS customer_id,
    a.cst_key           AS customer_number,
    a.cst_firstname     AS first_name,
    a.cst_lastname      AS last_name,
    c.cntry             AS country,
    a.cst_marital_status AS marital_status,
    CASE
        WHEN a.cst_gndr <> 'n/a' THEN a.cst_gndr
        ELSE COALESCE(b.gen, 'n/a')
    END                 AS gender,
    b.bdate             AS birthday,
    a.cst_create_date   AS create_date       
FROM silver.crm_cust_info a
LEFT JOIN silver.erp_cust_az12 b
    ON a.cst_key = b.cid
LEFT JOIN silver.erp_loc_a101 c
    ON a.cst_key = c.cid;
GO

/*==============================================================================
Create Dimension: gold.dim_product
==============================================================================*/
IF OBJECT_ID('gold.dim_product', 'V') IS NOT NULL
    DROP VIEW gold.dim_product;
GO

CREATE VIEW gold.dim_product AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY a.prd_start_dt, a.prd_key) AS product_key, -- Surrogate Key
    a.prd_id        AS product_id,
    a.prd_key       AS product_number,
    a.prd_nm        AS product_name,
    a.cat_id        AS category_id,
    b.cat           AS category,
    b.subcat        AS subcategory,
    b.maintenance   AS maintenance,
    a.prd_cost      AS cost,
    a.prd_line      AS product_line,
    a.prd_start_dt  AS start_date
FROM silver.crm_prd_info a
LEFT JOIN silver.erp_px_cat_g1v2 b
    ON a.cat_id = b.id
WHERE a.prd_end_dt IS NULL;   -- Keep only active products
GO

/*==============================================================================
Create Fact Table: gold.fact_sales
==============================================================================*/
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO

CREATE VIEW gold.fact_sales AS
SELECT
    s.sls_ord_num      AS order_number,
    p.product_key     AS product_key,     -- FK to dim_product
    c.customer_key    AS customer_key,    -- FK to dim_customers
    s.sls_order_dt    AS order_date,
    s.sls_ship_dt     AS shipping_date,
    s.sls_due_dt      AS due_date,
    s.sls_sales       AS sales_amount,
    s.sls_quantity    AS quantity,
    s.sls_price       AS price
FROM silver.crm_sales_details s
LEFT JOIN gold.dim_product p
    ON s.sls_prd_key = p.product_number
LEFT JOIN gold.dim_customers c
    ON s.sls_cust_id = c.customer_id;
GO

