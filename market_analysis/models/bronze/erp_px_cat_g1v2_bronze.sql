-- Full truncate + reload
select * from {{ source("staging_erp", "erp_px_cat_g1v2") }}
