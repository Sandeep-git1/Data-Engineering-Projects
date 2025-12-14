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

