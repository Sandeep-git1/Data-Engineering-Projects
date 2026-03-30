{{ config(materialized="table") }}

with
    crm_cust_info_silver as (select * from {{ ref("crm_cust_info_silver") }}),
    erp_cust_az12_silver as (select * from {{ ref("erp_cust_az12_silver") }}),
    erp_loc_a101_silver as (select * from {{ ref("erp_loc_a101_silver") }})

select
    -- Deterministic Surrogate Key
    md5(cast(c.cst_key as varchar)) as customer_sk,

    -- Natural Keys
    c.cst_id as customer_id,
    c.cst_key as customer_key,

    -- Demographics
    c.cst_firstname as first_name,
    c.cst_lastname as last_name,
    c.cst_marital_status as marital_status,

    case
        when c.cst_gndr != 'n/a' then c.cst_gndr else coalesce(e.gen, 'n/a')
    end as gender,

    e.bdate as birth_date,

    -- Geography
    l.cntry as country,

    -- Metadata
    c.cst_create_date as create_date

from crm_cust_info_silver c
left join erp_cust_az12_silver e on c.cst_key = e.cid
left join erp_loc_a101_silver l on c.cst_key = l.cid
