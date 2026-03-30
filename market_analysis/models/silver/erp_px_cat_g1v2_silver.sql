{{ config(materialized="table") }} select * from {{ ref("erp_px_cat_g1v2_bronze") }}
