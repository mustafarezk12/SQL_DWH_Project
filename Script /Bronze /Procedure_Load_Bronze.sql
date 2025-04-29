/*
SQL runs the TRY block, and if it fails , it runs the CATCH bock to handle the erro 
Track ETL duration helps to identitfy the bottlenecks,monitor performance,detect issues
*/



CREATE or ALTER Procedure bronze.load_bronze  AS
begin
	Declare @start_time datetime , @end_time datetime
	begin try
		print '==========================================';
		print'Loading Bronze Layer';
		print '==========================================';

		print '--------------------------------------------';
		print 'Loading CRM Tables';
		print '--------------------------------------------';
		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.crm_cust_info';
		Truncate table bronze.crm_cust_info;
		print'>> Inserting data into: bronze.crm_cust_info';
		Bulk Insert bronze.crm_cust_info
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'

		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.crm_prd_inf';
		Truncate table bronze.crm_prd_inf;
		print'>> Inserting data into: bronze.crm_prd_inf';
		Bulk Insert bronze.crm_prd_inf
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'

		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.crm_Sales_details';
		Truncate table bronze.crm_Sales_details;
		print'>> Inserting data into: bronze.crm_Sales_details';
		Bulk Insert bronze.crm_Sales_details
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'


		print '--------------------------------------------';
		print 'Loading ERP Tables';
		print '--------------------------------------------';
		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.erp_cust_az12';
		Truncate table bronze.erp_cust_az12;
		print'>> Inserting data into: bronze.erp_cust_az12';
		Bulk Insert bronze.erp_cust_az12
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'
	
		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.erp_loc_a101';
		Truncate table bronze.erp_loc_a101;
		print'>> Inserting data into: bronze.erp_loc_a101';
		Bulk Insert bronze.erp_loc_a101
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'
	
		set @start_time = GETDATE();
		print'>> Truncateing Table: bronze.erp_px_cat_g1v2';
		Truncate table bronze.erp_px_cat_g1v2;
		print'>> Inserting data into : bronze.erp_px_cat_g1v2';
		Bulk Insert bronze.erp_px_cat_g1v2
		from 'E:\iti\DWH_Project\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		with(
		firstrow = 2,
		fieldterminator = ',' ,
		tablock
		)
		set @end_time = GETDATE();
		print'>> Loading Duration: ' + cast( datediff(second, @start_time,@end_time) AS nvarchar) + 'seconds';
		print'>>>>---------------------------------'

	end try
	begin catch 

		print'================================'
		print 'Error occured during loading bronze layer'
		print 'Error message' + ERROR_MESSAGE();
		print 'Error state' + CAST(ERROR_MESSAGE() as NVARCHAR);
		print 'Error state' + CAST(ERROR_STATE() as NVARCHAR);
		print'================================'
	end catch 
END

