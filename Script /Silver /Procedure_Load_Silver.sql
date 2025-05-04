

Create or alter procedure silver.load_silver as
BEGIN 
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME; 
    
        SET @batch_start_time = GETDATE();
        PRINT '================================================';
        PRINT 'Loading Silver Layer';
        PRINT '================================================';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

	  SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into: silver.crm_cust_info';
/*
<<Clean & Load to silver.crm_cust_info>>
check for null & duplicates in primary key 
the expected result : NO Result But there are duplicates and null 
<<for duplicates>> select the fresh record by using row_number() <<for null>>  IS NOT NULL in condition
Quality check : check for unwanted spaces in string values
use <<TRIM()>> remove leading and trailing spaces from string
standerize cst_gndr gender values to Male & Female 
standerize cst_marital_status values to Single & married 
*/


		INSERT INTO silver.crm_cust_info (
		cst_id, 
		cst_key, 
		cst_firstname, 
		cst_lastname, 
		cst_marital_status, 
		cst_gndr,
		cst_create_date
		)
		select 
		cst_id,cst_key,trim(cst_firstname) as cst_firstname ,trim(cst_lastname) as cst_lastname ,
		case when UPPER(TRim(cst_gndr))= 'F' then 'Fmale'-- Normalize the values 
			 when UPPER(TRim(cst_gndr))= 'M' then 'Male'
			else  'n/a'
		end cst_gndr,

		case when UPPER(TRim(cst_marital_status))= 'S' then 'Single' -- Normalize the values 
			when UPPER(TRim(cst_marital_status))= 'M' then 'Married'
			else  'n/a'
		end cst_marital_status,
		cst_create_date
		from (
		select*,
		ROW_NUMBER() over (partition by cst_id order by cst_create_date desc) as flag_last_date
		from bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
		 )t where flag_last_date = 1 -- select most recent customer 
	SET @end_time = GETDATE()
	PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
	 PRINT '>> -------------';

	 SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into: silver.crm_prd_info';

/*
<< claen & load to  silver.crm_cust_info>>
check for null & duplicates in primary key  >> NON
<<deriving new 2 columns from prd_key>>
-->using substring(colmn,position to extract,characters_num) that extracts a specific part of string value 
Quality check : check for unwanted spaces in string values >> NON
--check for negative number in prd_cost & nulls >> no negative but have nulls -> use isnull() or coalesc() replace by (0)
--standerize prd_line values to friendly names >> case when ideal for simple value mapping
-- in columns prd_start_dt , prd_end_date there were overlapping between two dates. 
[solution]>> derive the end date from the start date as End Date = Start Date of the next record
 
*/
		
		
		
		INSERT INTO silver.crm_prd_info (
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
			REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id, -- Extract category ID
			SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,        -- Extract product key
			prd_nm,
			ISNULL(prd_cost, 0) AS prd_cost,
			CASE 
				WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
				WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
				WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
				WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
				ELSE 'n/a'
			END AS prd_line, -- Map product line codes to descriptive values
			CAST(prd_start_dt AS DATE) AS prd_start_dt,
			CAST(
				LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) 
				AS DATE
			) AS prd_end_dt -- Calculate end date as one day before the next start date
		FROM bronze.crm_prd_inf;
	SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

	SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into: silver.crm_sales_details';

/*
<< claen & load to  silver.crm_sales_details>>
-- the date formate in columns [sls_due_dt - sls_ship_dt- sls_order_dt] had problem as the the data type were INT not date 
--> change the dtype from INT to varchar and from varchar to date
there were a bad date quality(nulls-negative) in columns [sls_sales - sls_price] 
--> Recalculate sales if original value is missing or incorrect
*/
		INSERT INTO silver.crm_sales_details (
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		sls_order_dt,
		sls_ship_dt,
		sls_due_dt,
		sls_sales,
		sls_quantity,
		sls_price
		)
		SELECT 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE 
			WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
		END AS sls_order_dt,
		CASE 
			WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
		END AS sls_ship_dt,
		CASE 
			WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
		END AS sls_due_dt,
		CASE 
			WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price) 
				THEN sls_quantity * ABS(sls_price)
			ELSE sls_sales
		END AS sls_sales, -- Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE 
			WHEN sls_price IS NULL OR sls_price <= 0 
				THEN sls_sales / NULLIF(sls_quantity, 0)
			ELSE sls_price  -- Derive price if original value is invalid
		END AS sls_price
		FROM bronze.crm_sales_details;
	SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time = GETDATE();

		PRINT '>> Truncating Table: silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into: silver.erp_cust_az12'
/*
<< claen & load to silver.erp_az12>>
-- the cid in bronze.erp_az12 not match the cid in crm there was an 'NAS' addition in bronze.erp_az12
--> delete the addition characters 'NAS'
-the there were dates in bdate in the future 
--> replace the future dates with nulls 
-there were a bad date quality(nulls-negative) in columns [sls_sales - sls_price] 
--> Recalculate sales if original value is missing or incorrect
-in the gen column there were multipple vaues [F,M,NULL,Female,Male]
--> Standerdize the values into [Male , Female , n/a]
*/
		insert into silver.erp_cust_az12(cid,bdate,gen)
			select 
			case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid)) 
			else cid 
			end  as cid,
			 case when bdate > GETDATE() then null 
			 else bdate
			 end as bdate,
			case when upper(trim(gen)) in ('F' , 'Female') then 'Female'
				when upper(trim(gen)) in ('M' , 'Male') then 'Male'
			else 'n/a'
			end as gen
		from bronze.erp_cust_az12
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';


		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into: silver.erp_loc_a101';

/*<<load & clean to silver.erp_loc_a101>>
-in cntry columns there were missing & NULL & Multi object for the same countrey name 
--> hnadle missings and null ad 'n/a'
--> standerize the multi object to one object
*/
		insert into silver.erp_loc_a101(cid,cntry)
		select 
		replace(cid , '-','') as cid ,
		case when TRIM(cntry) = 'DE' then 'Germant'
			 when Trim(cntry) in ('US','USA') then 'United States'
			 when trim(cntry) = '' or cntry is null then 'n/a'
		else cntry
		end  cntry
		from bronze.erp_loc_a101
		SET @end_time = GETDATE();
			PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
			PRINT '>> -------------';

		SET @start_time = GETDATE();
		PRINT '>> Truncating Table: silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into: silver.erp_px_cat_g1v2';

/*<<load & clean to erp_px_cat_g1v2>>
CLeane with no issues
*/

		insert into silver.erp_px_cat_g1v2
		(id,cat,subcat,maintenance)
		select *
		from bronze.erp_px_cat_g1v2
		
	SET @end_time = GETDATE();
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds';
        PRINT '>> -------------';

end


EXEC Silver.load_silver;
