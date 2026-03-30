{{
    config(
        materialized="incremental",
        unique_key=["sls_ord_num", "sls_prd_key"],
        incremental_strategy="merge",
    )
}}

select
    sls_ord_num,
    sls_prd_key,  -- Line item identifier
    sls_cust_id,
    try_to_date(to_varchar(sls_order_dt)) as sls_order_dt,
    try_to_date(to_varchar(sls_ship_dt)) as sls_ship_dt,
    try_to_date(to_varchar(sls_due_dt)) as sls_due_dt,
    case
        when
            sls_sales is null
            or sls_sales <= 0
            or sls_sales != sls_quantity * abs(sls_price)
        then sls_quantity * abs(sls_price)
        else sls_sales
    end as sls_sales,
    sls_quantity,
    case
        when sls_price is null or sls_price <= 0
        then sls_sales / nullif(sls_quantity, 0)
        else sls_price
    end as sls_price
from {{ ref("crm_sales_details_bronze") }}
