-- FULL LOAD
create or alter procedure bronze.load_bronze as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		print '==================================='
		print 'Loading CRM Table'
		print '==================================='

		set @batch_start_time = GETDATE();
		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_cust_info'
		truncate table bronze.crm_cust_info --Loại bỏ hết dữ liệu trong bảng
		print '>> Inserting Data Into: bronze.crm_cust_info'
		bulk insert bronze.crm_cust_info --Load toàn bộ dữ liệu vào bảng
		from 'F:\DataWarehouse\source_crm\cust_info.csv'
		with(
			firstrow = 2, -- = header
			fieldterminator = ',', -- = delimiter
			tablock -- Khóa bảng tránh nhiều truy cập thay đổi cùng 1 lúc để đảm bảo toàn vẹn dữ liệu
		)
		set @end_time = GETDATE();
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_prd_info'
		truncate table bronze.crm_prd_info
		print '>> Inserting Data Into: bronze.crm_prd_info'
		bulk insert bronze.crm_prd_info
		from 'F:\DataWarehouse\source_crm\prd_info.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.crm_sales_details'
		truncate table bronze.crm_sales_details
		print '>> Inserting Data Into: bronze.crm_sales_details'
		bulk insert bronze.crm_sales_details
		from 'F:\DataWarehouse\source_crm\sales_details.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)

		print '==================================='
		print 'Loading CRM Table'
		print '==================================='

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_cust_az12'
		truncate table bronze.erp_cust_az12
		print '>> Inserting Data Into: bronze.erp_cust_az12'
		bulk insert bronze.erp_cust_az12
		from 'F:\DataWarehouse\source_erp\CUST_AZ12.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_loc_a101'
		truncate table bronze.erp_loc_a101
		print '>> Inserting Data Into: bronze.erp_loc_a101'
		bulk insert bronze.erp_loc_a101
		from 'F:\DataWarehouse\source_erp\LOC_A101.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @start_time = GETDATE();
		print '>> Truncating Table: bronze.erp_px_cat_g1v2'
		truncate table bronze.erp_px_cat_g1v2
		print '>> Inserting Data Into: bronze.erp_px_cat_g1v2'
		bulk insert bronze.erp_px_cat_g1v2
		from 'F:\DataWarehouse\source_erp\PX_CAT_G1V2.csv'
		with(
			firstrow = 2,
			fieldterminator = ',',
			tablock
		)
		set @end_time = GETDATE();
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @batch_end_time = GETDATE()
		print '==================================='
		print 'Loading Bronze Layer Successful. Total Time Execute: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		print '==================================='

	end try
	begin catch
		print '==================================='
		print 'Failed to loading bronze layer. Error Message:' + error_message();
		print 'Failed to loading bronze layer. Error Message:' + cast(error_number() as nvarchar);
		print 'Failed to loading bronze layer. Error Message:' + cast(error_state() as nvarchar)
		print '==================================='
	end catch
end