{{ config(materialized="incremental", unique_key="cst_id") }}

with
    latest_customers as (
        select
            *,
            row_number() over (partition by cst_id order by cst_create_date desc) as rn
        from {{ ref("crm_cust_info_bronze") }}
        where cst_id is not null
    )

select
    cst_id,
    cst_key,
    {{ trim("cst_firstname") }} as cst_firstname,
    {{ trim("cst_lastname") }} as cst_lastname,
    {{ normalize_marital_status("cst_marital_status") }} as cst_marital_status,
    {{ normalize_gender("cst_gndr") }} as cst_gndr,
    cst_create_date

from latest_customers
where rn = 1
