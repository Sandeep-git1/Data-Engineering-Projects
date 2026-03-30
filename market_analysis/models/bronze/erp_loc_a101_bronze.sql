-- Full truncate + reload
select * from {{ source("staging_erp", "erp_loc_a101") }}
