/*
STORED PROCEDURE : Load Bronze Layer (Source -> Bronze)
*/

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
	BEGIN TRY
	SET @batch_start_time = GETDATE();
		PRINT 'Loading Bronze Layer'
		PRINT '---------------------'

		PRINT 'Loading CRM Tables'
		PRINT '-------------------'

		SET @start_time =GETDATE();
		TRUNCATE TABLE bronze.crm_cust_info

		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\cust_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>LOAD Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\prd_info.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT '-----------------'

		SET @start_time = GETDATE();
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_crm\sales_details.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time = GETDATE();
		PRINT '>>LOAD Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT '-----------------'

		PRINT 'Loading ERP Tables'
		PRINT '-------------------'

		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\CUST_AZ12.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @end_time =GETDATE();
		PRINT '>>LOAD Duration: ' + cast(datediff(second,@start_time,@end_time) as nvarchar) + 'seconds';
		PRINT '-----------------'

		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\LOC_A101.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);

		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\annuj\Downloads\sql-data-warehouse-project-main\sql-data-warehouse-project-main\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
		);
		SET @batch_end_time = GETDATE();
		PRINT'=============================================';
		PRINT'Loading Bronze Layer is completed';
		PRINT'============================================='
	END TRY
	BEGIN CATCH
	    PRINT '=========================================='
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER'
		PRINT '============================================'
	END CATCH
END
