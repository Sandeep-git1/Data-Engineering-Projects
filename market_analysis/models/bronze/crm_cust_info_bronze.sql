-- models/bronze/crm_cust_info_bronze.sql 
-- Full truncate + reload
select * from {{ source("staging_crm", "crm_cust_info") }}
