-- Full truncate + reload
select * from {{ source("staging_crm", "crm_sales_details") }}
