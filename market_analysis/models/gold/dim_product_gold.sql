{{ config(materialized="table") }}

with
    crm_prd_info_silver as (select * from {{ ref("crm_prd_info_silver") }}),
    erp_px_cat_g1v2_silver as (select * from {{ ref("erp_px_cat_g1v2_silver") }})

select
    -- Deterministic Surrogate Key
    md5(cast(p.prd_key as varchar)) as product_sk,

    -- Natural Keys
    p.prd_id as product_id,
    p.prd_key as product_number,

    -- Product Details
    p.prd_nm as product_name,
    p.prd_cost as cost,
    p.prd_line as product_line,

    -- Categorization
    c.cat as category,
    c.subcat as subcategory,
    c.maintenance as maintenance,

    p.prd_start_dt as start_date

from crm_prd_info_silver p
left join erp_px_cat_g1v2_silver c on p.cat_id = c.id
where p.prd_end_dt is null
