-- Full truncate + reload
select * from {{ source("staging_erp", "erp_cust_az12") }}
