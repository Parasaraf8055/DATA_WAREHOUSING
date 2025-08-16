
/*

============================================================================================================

--- STORED PROCEDURE TO LOAD BRONZE LAYER - (SOUCE - BRONZE) -- BULK INSERT INTO BRONZE LAYERS TABLES CREATED AS STORED PROCEDURE

--- PURPOSE - STORED PROCEDURE TO LOAD THE DATA INTO BRONZE SCHEMA FROM EXTERNAL CSV FILE
            - TRUNCATE THE TABLE BEFORE STORING
            - USE BULK INSERT TO LOAD THE DATA FASTER
--- PARAMETERS - NONE

HOW TO USE - EXEC Bronze.load_bronze;



*/

CREATE OR ALTER PROCEDURE Bronze.load_bronze AS
BEGIN
	
	DECLARE @start_time DATETIME, @end_time DATETIME, @b_start_time DATETIME, @b_end_time DATETIME;
	BEGIN TRY
	PRINT '====================================';
	PRINT 'LOADING THE BRONZE LAYER';
	PRINT '====================================';

	SET @b_start_time = GETDATE();
	PRINT '---------------------------------------------------------------------';
	PRINT 'LOADING THE CRM TABLES';
	PRINT '---------------------------------------------------------------------';

	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_cust_info;

	BULK INSERT Bronze.crm_cust_info
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';

	

	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_prd_info;

	BULK INSERT Bronze.crm_prd_info
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';


	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.crm_sales_details;

	BULK INSERT Bronze.crm_sales_details
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';



	PRINT '---------------------------------------------------------------------';
	PRINT 'LOADING THE ERP TABLES ';
	PRINT '---------------------------------------------------------------------';


	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_cust_az12;

	BULK INSERT Bronze.erp_cust_az12
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';


	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_loc_a101;


	BULK INSERT Bronze.erp_loc_a101
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);
	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';


	SET @start_time = GETDATE();
	TRUNCATE TABLE Bronze.erp_px_cat_g1v2;


	BULK INSERT Bronze.erp_px_cat_g1v2
	FROM 'C:\Users\PARAS\Downloads\sql-data-warehouse-project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
	WITH (
		FIRSTROW = 2,
		FIELDTERMINATOR = ',',
		TABLOCK
	);

	SET @end_time = GETDATE();

	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';

	SET @b_end_time = GETDATE();

	PRINT '>> LOADING THE BRONZE LAYER IS COMPLETED -----------#############';

	PRINT '>> TOTAL TIME REQUIRED BY BRONZE LAYER TO LOAD THE DATA :'
	
	PRINT '>> LOAD DURATION : '+ CAST(DATEDIFF(SECOND, @b_start_time, @b_end_time) AS NVARCHAR) + ' seconds';

	PRINT '------------------------------------------------------------------------------------------';

	END TRY
	BEGIN CATCH
		PRINT '==========================================';
		PRINT ' ERROR OCCURED DURING LOADING OF BRONZE LAYER';
		PRINT ' ERROR MESSAGE'+ ERROR_MESSAGE();
		PRINT ' ERROR NUMBER '+ CAST(ERROR_NUMBER() AS NVARCHAR);
		--- TRACK ETL DURATIONS HELP TO OPTIMIZE' 
	END CATCH
END
