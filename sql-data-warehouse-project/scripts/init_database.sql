/*
================================================================================
Create Database and Schemas
================================================================================

Script Purpose:
This script creates a new database named 'DataWarehouse' after checking if it already exists.
If the database exists, it is dropped and recreated. Additionally, the script sets up three schemas
within the database: 'bronze', 'silver', and 'gold'.

WARNING:
Running this script will drop the entire 'DataWarehouse' database if it exists.
All data in the database will be permanently deleted. Proceed with caution
and ensure you have proper backups before running this script.
*/

--Create Database 'Datawarehouse'

USE master;
GO

  --Drop and recreate the "DataWarehouse' database
  IF EXISTS (SELECT 1 FROM sys.databases WHERE name='DataWarehouse')
  BEGIN 
    ALTER DATABASE DataWarehouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
    DROP DATABASE DataWarehouse;
  END;
GO

CREATE DATABASE DataWarehouse;

USE DataWarehouse;

CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;
GO


USE DataWarehouse;

/* WE ARE USING "OBJECT_ID" TO CHECK IS THE TABLE EXIST - IF YES, DROP THAT TABLE AND CREATE A NEW ONE, ELSE CREATE TABLE
INSDIE THE DATAWAREHOUSE*/
IF OBJECT_ID(' bronze.crm_cust_ifno', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.crm_cust_ifno;
CREATE TABLE bronze.crm_cust_ifno(
cst_id INT,
cst_key NVARCHAR(50),
cst_firstname NVARCHAR(50),
cst_lastname NVARCHAR(50),
cst_marital_status NVARCHAR(50),
cst_gndr NVARCHAR(50),
cst_create_date DATE
);

IF OBJECT_ID('bronze.crm_sales_details', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.crm_sales_details;
CREATE TABLE bronze.crm_sales_details(
sls_ord_num NVARCHAR(50),
sls_prd_key NVARCHAR(50),
sls_cust_id INT,
sls_order_dt INT,
sls_ship_dt INT,
sls_due_dt INT,
sls_sales INT,
sls_quantity INT,
sls_price INT,
);

IF OBJECT_ID('bronze.crm_prd_info', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.crm_prd_info;
CREATE TABLE bronze.crm_prd_info(
prd_id INT,
prd_key NVARCHAR(50),
prd_nm NVARCHAR(50),
prd_cost INT,
prd_line NVARCHAR(50),
prd_start_dt DATETIME,
prd_end_dt DATETIME
);

IF OBJECT_ID('bronze.erp_cust_az12', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.erp_cust_az12;
CREATE TABLE bronze.erp_cust_az12(
cid INT, 
bdate DATE,
gen NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_loc_a101', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.erp_loc_a101;
CREATE TABLE bronze.erp_loc_a101(
cid NVARCHAR(50),
cntry NVARCHAR(50)
);

IF OBJECT_ID('bronze.erp_px_cat_g1v2', 'U') IS NOT NULL /* 'U' HERE IS THE USER */
	DROP TABLE bronze.erp_px_cat_g1v2;
CREATE TABLE bronze.erp_px_cat_g1v2(
id NVARCHAR(50),
cat NVARCHAR(50),
subcat NVARCHAR(50),
maintenance NVARCHAR(50)
);


/* to rename if there is an spelling error-
EXEC sp_rename 'bronze.crm_cust_ifno', 'crm_cust_info';
*/

/* to alter table if you input wrong datatype - 
ALTER TABLE bronze.erp_cust_az12
ALTER COLUMN cid VARCHAR(50);
*/

/* # this is the sql script to use "truncate" and "full load" the data */
use DataWarehouse;

TRUNCATE TABLE bronze.crm_cust_info;
BULK INSERT bronze.crm_cust_info
from 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
with (
 firstrow = 2,
 FIELDTERMINATOR = ',',
 tablock  --Ensures the load is treated as a single, atomic table operation
);

SELECT COUNT(*) FROM bronze.crm_cust_info;

TRUNCATE TABLE bronze.crm_prd_info;
BULK INSERT bronze.crm_prd_info
from 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
with (
 firstrow = 2,
 FIELDTERMINATOR = ',',
 tablock
);

SELECT COUNT(*) FROM bronze.crm_cust_info;


TRUNCATE TABLE bronze.crm_sales_details;
BULK INSERT bronze.crm_sales_details
from 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
with (
 firstrow = 2,
 FIELDTERMINATOR = ',',
 tablock
);
SELECT COUNT(*) FROM bronze.crm_sales_details;

ALTER TABLE bronze.erp_cust_az12
ALTER COLUMN cid VARCHAR(50);


TRUNCATE TABLE bronze.erp_cust_az12;
BULK INSERT bronze.erp_cust_az12
FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
SELECT COUNT(*) FROM bronze.erp_cust_az12;

TRUNCATE TABLE bronze.erp_loc_a101;
BULK INSERT bronze.erp_loc_a101
FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
SELECT COUNT(*) FROM bronze.erp_loc_a101;

TRUNCATE TABLE bronze.erp_px_cat_g1v2;
BULK INSERT bronze.erp_px_cat_g1v2
FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    TABLOCK
);
SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;

-- FOR COMPLETENESS PLEASE REFER TO THE BELOW CODE 

-----------------------------------------------------------------------------
--DATA INJECTION AND COMPLETENESS--
-----------------------------------------------------------------------------
/* # Bronze Layer Load Procedure

## Overview
This stored procedure (`bronze.load_bronze`) is responsible for loading **raw source data** into the **Bronze layer** of the SQL Data Warehouse.  
The Bronze layer acts as the **landing zone** for source-system data and preserves it **as-is** for traceability and debugging.

---
## Purpose
- Perform **full refresh loads** from source CSV files
- Maintain **idempotent execution** (safe to re-run)
- Capture **load durations** at both table and batch level
- Provide **basic operational logging** via `PRINT` statements
- Support **data lineage** from source → Bronze layer

---
## Tables Loaded

### CRM Source
- `bronze.crm_cust_info`
- `bronze.crm_prd_info`
- `bronze.crm_sales_details`

### ERP Source
- `bronze.erp_cust_az12`
- `bronze.erp_loc_a101`
- `bronze.erp_px_cat_g1v2`

---
## Load Strategy
- Each table is **truncated** before loading
- Data is ingested using `BULK INSERT`
- CSV headers are skipped using `FIRSTROW = 2`
- Loads are **full reloads**, ensuring no duplicate data on re-runs

---
## Execution Flow
1. Capture batch start time
2. Load CRM tables sequentially
3. Load ERP tables sequentially
4. Measure and print:
   - Per-table load duration
   - Total batch duration
5. Handle errors using `TRY...CATCH`

---
## Error Handling
- Errors are caught using a `TRY...CATCH` block
- On failure, the procedure prints:
  - Error message
  - Error number
- This allows quick diagnosis during development and testing

---
## Performance Tracking
- Table-level load time is captured using `DATEDIFF (millisecond)`
- Batch-level duration is printed at the end of execution
- Useful for identifying slow loads or file issues

---
## How to Run

```sql
EXEC bronze.load_bronze; 
THIS SQL CODE SECTION IS BELOW */ 


CREATE OR ALTER PROCEDURE bronze.load_bronze
AS
BEGIN
    DECLARE 
        @start_time DATETIME,
        @end_time DATETIME,
        @batch_start_time DATETIME,
        @batch_end_time DATETIME;

    BEGIN TRY 
        SET @batch_start_time = GETDATE();

        PRINT '===========================================================';
        PRINT 'Loading Bronze layer';
        PRINT '===========================================================';

        PRINT '***********************************************************';
        PRINT 'Loading CRM Tables';
        PRINT '***********************************************************';

        -- CRM CUSTOMER
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.crm_cust_info;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

        -- CRM PRODUCT
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data Into: bronze.crm_prd_info';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.crm_prd_info;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

        -- CRM SALES
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;
        PRINT '>>Inserting Data Into: bronze.crm_sales_details';
        BULK INSERT bronze.crm_sales_details
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.crm_sales_details;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

        PRINT '***********************************************************';
        PRINT 'Loading ERP Tables';
        PRINT '***********************************************************';

        -- ERP CUSTOMER
        SET @start_time = GETDATE();
        PRINT '>>Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>>Inserting Data Into: bronze.erp_cust_az12';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.erp_cust_az12;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

        -- ERP LOCATION
        SET @start_time = GETDATE();
        PRINT '>>Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>>Inserting Data Into: bronze.erp_loc_a101';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.erp_loc_a101;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

        -- ERP PRODUCT CATEGORY
        SET @start_time = GETDATE();
        PRINT '>>Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>>Inserting Data Into: bronze.erp_px_cat_g1v2';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\project_\Data Engineering\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', TABLOCK);
        SELECT COUNT(*) FROM bronze.erp_px_cat_g1v2;
        SET @end_time = GETDATE();
        PRINT 'LOADING TABLE DURATION: ' 
              + CAST(DATEDIFF(millisecond, @start_time, @end_time) AS NVARCHAR(20)) 
              + ' MILLISECOND';

    END TRY
    BEGIN CATCH
        PRINT '***********************************************************';
        PRINT 'Error Occured During Loading Bronze Layer';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR(20));
        PRINT '***********************************************************';
    END CATCH;

    SET @batch_end_time = GETDATE();
    PRINT 'LOADING BATCH DURATION: ' 
          + CAST(DATEDIFF(millisecond, @batch_start_time, @batch_end_time) AS NVARCHAR(20)) 
          + ' MILLISECOND';
END;
GO

EXEC bronze.load_bronze


