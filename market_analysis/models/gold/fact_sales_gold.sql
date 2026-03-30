{{ config(materialized="table") }}

with crm_sales_details_silver as (select * from {{ ref("crm_sales_details_silver") }})

select
    -- Fact Table Primary Key (Order Number + Product Key)
    md5(concat(cast(sls_ord_num as varchar), cast(sls_prd_key as varchar))) as sales_sk,

    -- Deterministic Foreign Keys for joining to Dimensions
    md5(cast(sls_cust_id as varchar)) as customer_sk,
    md5(cast(sls_prd_key as varchar)) as product_sk,

    -- Natural Keys
    sls_ord_num as order_number,

    -- Dates
    sls_order_dt as order_date,
    sls_ship_dt as ship_date,
    sls_due_dt as due_date,

    -- Metrics
    sls_quantity as quantity,
    sls_price as price,
    sls_sales as sales_amount

from crm_sales_details_silver
