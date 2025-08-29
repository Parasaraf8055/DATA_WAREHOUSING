
-----------CREATE A SP FOR A SILVER LAYER TO TRANFORM AND STORE THE DATA

EXEC Silver.load_silver;


CREATE OR ALTER PROCEDURE Silver.load_silver AS
BEGIN
	
	DECLARE @BATCH_START_TIME DATETIME, @BATCH_END_TIME DATETIME;

	SET 
	@BATCH_START_TIME = GETDATE();

	---  checks for nulls and duplicates in pk
	-- exception : no result
	PRINT '===================================================================================';
	PRINT 'LOADING THE SILVER LAYER';
	PRINT '===================================================================================';
	/*
	SELECT 
	cst_id,
	COUNT(*) FLAG
	FROM Bronze.crm_cust_info
	GROUP BY cst_id
	HAVING COUNT(*) > 1 OR cst_id IS NULL


	
	SELECT 
	* 
	FROM Bronze.crm_cust_info
	WHERE cst_id = 29466
	*/
	/*
	-- this will give you all unique value for pk
	SELECT 
	*
	FROM(
		SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)T WHERE flag_last = 1
	*/

	-- checking for unwanted spaces for categorials 
	-- EXPECTED - NO RESLTS 
	/*
	SELECT
	cst_firstname
	FROM Bronze.crm_cust_info
	WHERE cst_firstname != TRIM(cst_firstname)

	-- GOT SOME IN FIRSTNAME

	SELECT
	cst_lastname
	FROM Bronze.crm_cust_info
	WHERE cst_lastname != TRIM(cst_lastname)

	-- GOT SOME IN LASTNAME

	SELECT
	cst_gndr
	FROM Bronze.crm_cust_info
	WHERE cst_gndr != TRIM(cst_gndr)

	-- NO RESULT FOUND

	-- CHECKING THE DATA STANDARDIZATION AND CONSISTANCY

	SELECT DISTINCT cst_gndr
	FROM Bronze.crm_cust_info

	*/
	--- CLEANING THE SPACES AND REMOVE DUPLICATES THEN INSERTED INTO SILVER.CRM_CUST_INFO

	PRINT '===================================================================================';
	PRINT 'LOADING THE CRM TABLES';
	PRINT '===================================================================================';

	PRINT '>> TRUNCATING TABLE : SILVER.CRM_CUST_INFO';
	TRUNCATE TABLE Silver.crm_cust_info;
	PRINT '>> INSERTING INTO THE : SILVER.CRM_CUST_INFO';

	INSERT INTO Silver.crm_cust_info (
		cst_id,
		cst_key,
		cst_firstname,
		cst_lastname,
		cst_material_status,
		cst_gndr,
		cst_create_date

	)
	SELECT 
		cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status)) = 'S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status)) = 'M' THEN 'Married'
			 ELSE 'n/a'
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
			 ELSE 'n/a'
		END cst_gndr,

		cst_create_date

	FROM(
		SELECT 
		*,
		ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
		FROM Bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
	)T WHERE flag_last = 1


	--======================================================================================
	/*
	-- CHECKING FOR DUPLICATES 
	-- NO DUPLICATES
	SELECT 
	*
	FROM Bronze.crm_prd_info
	GROUP BY prd_id
	HAVING COUNT(*) > 1 OR prd_id IS NULL

	-----------------------------------------------------------------------------
	-- CHECKING FOR START AND END DATE 

	SELECT 
	prd_start_dt,
	prd_end_dt
	FROM Bronze.crm_prd_info
	WHERE prd_start_dt > prd_end_dt
	*/
	-----------------------------------------------------------------------------


	--- APPLYING TRANSFORMATIONS TO SILVER CRM_PRD_INFO COLUMN

	PRINT '>> TRUNCATING TABLE : Silver.crm_prd_info';
	TRUNCATE TABLE Silver.crm_prd_info;
	PRINT '>> INSERTING INTO THE : Silver.crm_prd_info';

	INSERT INTO Silver.crm_prd_info(
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt

	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			 ELSE 'n/a'
		END prd_line,

		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) -1 AS DATE) AS prd_end_dt

	FROM Bronze.crm_prd_info

	-- CHECKING HERE 

	--SELECT
	--*
	--FROM Silver.crm_prd_info

	--=======================================================================================================

	-------------------------------------------------------------------
	/*
	---  BRONZE.SALES_DETAILS

	--- CHECKING FOR INVALIDE DATES
	SELECT 
	NULLIF(sls_ord_dt,0) sls_ord_dt
	FROM Bronze.crm_sales_details
	WHERE sls_ord_dt <= 0
	OR LEN(sls_ord_dt) != 8
	OR sls_ord_dt > 473722372
	OR sls_ord_dt < 729348488

	----------------------------------------------------------------
	--- CHECHING FOR INVALIDE DATE ORDERS

	SELECT
	*
	FROM Bronze.crm_sales_details
	WHERE sls_ord_dt > sls_ship_dt OR sls_ord_dt > sls_due_dt
	-- NO INVALIDE DATES

	------------------------------------------------------------------

	--- BUSINESS RULE FOR SALES AND QUANTITY, PRICE

	--- SALES = QUANTITY * PRICE
	--- NEG, ZEROS , NULLS NOT ALLOWED

	SELECT 
	sls_sales,
	sls_quantity,
	sls_price
	FROM Bronze.crm_sales_details
	WHERE sls_sales != sls_quantity * sls_price
	OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
	OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0 
	ORDER BY sls_sales , sls_quantity , sls_price
	*/


	--- TRANFORMATIONS ON BRONZE.SALES_DETAILS AND INSERT INTO THE TABLE

	PRINT '>> TRUNCATING TABLE : Silver.crm_sales_details';
	TRUNCATE TABLE Silver.crm_sales_details;
	PRINT '>> INSERTING INTO THE : Silver.crm_sales_details';

	INSERT INTO Silver.crm_sales_details(sls_ord_num, sls_prd_key, sls_cust_id, sls_ord_dt, sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price)
	SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_ord_dt = 0 OR LEN(sls_ord_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ord_dt AS VARCHAR) AS DATE)
		END sls_ord_dt,
		CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END sls_ship_dt,
		CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			 THEN sls_quantity * ABS(sls_price)
			 ELSE sls_sales
		END AS sls_sales,
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price
		END AS sls_price


	FROM Bronze.crm_sales_details


	-- CHECKING THE INSERTION

	--SELECT * 
	--FROM Silver.crm_sales_details


	--=====================================================================================================

	-- CHECKING AND TRANSFORMIN THE BRONZE.ERP_CUST_AZ12
	/*
	SELECT * 
	FROM Bronze.erp_cust_az12
	-- THIS 2 TABLES JOIN BY CID AND CST_KEY THEY ARE SAME BUT ONLY NAS IS EXTRA 
	SELECT *
	FROM Silver.crm_cust_info
	*/
	----------------------------------------------------------------------------

	--- CHECKING FOR OUTOF RANGE DATES
	/*
	SELECT DISTINCT 
	bdate
	FROM Bronze.erp_cust_az12
	WHERE bdate < '1924-01-01' OR bdate > GETDATE()
	*/
	-- YAHH THERE ARE DATES THAT ACTUALLY GRATER THAN TODAYS DATE

	-----------------------------------------------------------------------------

	--- ROMOVE  'NAS' FROM CID , FIXING THE OUT OF RANGE DATES , THEN INSERT INTO THE TABLE

	PRINT '===================================================================================';
	PRINT 'LOADING THE ERP TABLES';
	PRINT '===================================================================================';


	PRINT '>> TRUNCATING TABLE : Silver.erp_cust_az12';
	TRUNCATE TABLE Silver.erp_cust_az12;
	PRINT '>> INSERTING INTO THE : Silver.erp_cust_az12';


	INSERT INTO Silver.erp_cust_az12 (cid,bdate,gen)
	SELECT
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
			 ELSE cid
		END cid,
		CASE WHEN bdate > GETDATE() THEN NULL
			 ELSE bdate
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN  ('M','MALE') THEN 'MALE'
			 WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'FEMALE'
			 ELSE 'n/a'
		END AS gen

	FROM Bronze.erp_cust_az12

	-- CHECKING THE INSERTIONS
	--SELECT * 
	--FROM Silver.erp_cust_az12


	--===================================================================

	-- CHECKING AND TRANSFORMING FOR SILVER ERP_LOC_A101 IT HAVING THE COUNTRY INFO
	/*
	SELECT 
	*
	FROM Bronze.erp_loc_a101

	-- CHECKING FOR DATA QUATILY IN COUNTRY

	SELECT DISTINCT cntry
	FROM Bronze.erp_loc_a101
	ORDER BY cntry

	*/
	------------------------------------------------------------------


	PRINT '>> TRUNCATING TABLE : Silver.erp_loc_a101';
	TRUNCATE TABLE Silver.erp_loc_a101;
	PRINT '>> INSERTING INTO THE :Silver.erp_loc_a101';

	INSERT INTO Silver.erp_loc_a101(cid,cntry)
	SELECT 
	REPLACE(cid, '-', '') AS cid,
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END AS cntry
	FROM Bronze.erp_loc_a101



	--========================================================================

	-- TRANSFORMING LAST TABLE ERP_PX_CAT_G1V2

	--SELECT *
	--FROM Bronze.erp_px_cat_g1v2

	-----------------------------------------------------

	-- CHECKING FOR UNWANTED SPACE

	--SELECT * FROM Bronze.erp_px_cat_g1v2
	----WHERE cat != TRIM(cat) OR subcat != TRIM(subcat)
	-- NO RESULT FOUND

	----------------------------------------------------------

	-- CHECKING FOR CONSISTANCY

	--SELECT DISTINCT subcat FROM Bronze.erp_px_cat_g1v2

	--- ALL GOOD NO NEED TO TRANSFORM DIRECTLY INSERTING

	-------------------------------------------------------------

	PRINT '>> TRUNCATING TABLE : Silver.erp_px_cat_g1v2';
	TRUNCATE TABLE Silver.erp_px_cat_g1v2;
	PRINT '>> INSERTING INTO THE :Silver.erp_px_cat_g1v2';

	INSERT INTO Silver.erp_px_cat_g1v2(id,cat,subcat,maintenance)
	SELECT 
	id,
	cat,
	subcat,
	maintenance
	FROM Bronze.erp_px_cat_g1v2

	-- CHECKING 
	--SELECT * FROM Silver.erp_px_cat_g1v2

	SET @BATCH_END_TIME = GETDATE();

	PRINT '>> START TIME : '+ CAST(@BATCH_START_TIME AS NVARCHAR)
	PRINT '>> END TIME : '+ CAST(@BATCH_END_TIME AS NVARCHAR)

END;


