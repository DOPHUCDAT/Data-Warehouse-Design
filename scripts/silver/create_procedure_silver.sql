create or alter procedure silver.load_silver as
begin
	declare @start_time datetime, @end_time datetime, @batch_start_time datetime, @batch_end_time datetime
	begin try
		print '==================================='
		print 'Loading ERP Table'
		print '==================================='

		set @batch_start_time = GETDATE();

		-- Silver.crm_cust_info
		set @start_time = GETDATE();
		print '>> Truncating Table: silver.crm_cust_info'
		truncate table silver.crm_cust_info
		print '>> Inserting Data Into: silver.crm_cust_info'
		insert into silver.crm_cust_info(
			cst_id,
			cst_key,
			cst_firstname,
			cst_lastname,
			cst_gender,
			cst_marital_status,
			cst_create_date
		)

		select cst_id,cst_key, trim(cst_firstname) as cst_firstname, trim(cst_lastname) as cst_lastname,
		case when upper(cst_gender) = 'F' then 'Female'
			when upper(cst_gender) = 'M' then 'Male'
			else 'N/A'
		end cst_gender,
		case when upper(cst_marital_status) = 'M' then 'Married'
			when upper(cst_marital_status) = 'S' then 'Single'
			else 'N/A'
		end cst_material_status,
		cst_create_date

		-- Loại bỏ các id không phải độc nhất
		from (
			select *, row_number() over(partition by cst_id order by cst_create_date) as flag_last
			from bronze.crm_cust_info
			where cst_id is not null
		)
		as cleanned 
		where flag_last = 1
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		--Silver.crm_prd_info
		set @start_time = GETDATE()
		print '>> Trucating Table: silver.crm_prd_info'
		truncate table silver.crm_prd_info
		print '>> Inserting Data Into: silver.crm_prd_info'
		insert into silver.crm_prd_info(
			prd_id,
			cat_id,
			prd_key,
			prd_nm,
			prd_cost,
			prd_line,
			prd_start_dt,
			prd_end_dt
		)

		select prd_id,
		replace(substring(prd_key,1,5), '_', '-') as cat_id,
		substring(prd_key,7, len(prd_key)) as prd_key,
		prd_nm,
		isnull(prd_cost, 0) as prd_cost,
		case upper(trim(prd_line)) 
			when 'R' then 'Road'
			when 'S' then 'Other Sales'
			when 'T' then 'Tour'
			when 'M' then 'Mountain'
		else 'N/A'
		end as prd_line,
		cast(prd_start_dt as date) as prd_start_dt,
		lead(prd_start_dt) over(partition by prd_key order by prd_start_dt) - 1 as prd_end_dt 
		from bronze.crm_prd_info
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		--Silver.crm_sales_details
		set @start_time = GETDATE()
		print '>> Truncating Table: silver.crm_sales_details'
		truncate table silver.crm_sales_details
		print '>> Inserting Data Into: silver.crm_prd_info'
		insert into silver.crm_sales_details(
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

		select sls_ord_num, sls_prd_key, sls_cust_id,
		case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
			else cast(cast(sls_order_dt as varchar) as date)
		end as sls_order_dt,
		case when sls_ship_dt = 0 or len(sls_ship_dt)  != 8 then null
			else cast(cast(sls_ship_dt as varchar) as date)
		end as sls_ship_dt,
		case when sls_due_dt = 0 or len(sls_due_dt) != 8 then NULL
			else cast(cast(sls_due_dt as varchar) as date)
		end as sls_due_dt,
		case when sls_sales <= 0 or sls_sales is null or sls_sales != abs(sls_price) * sls_quantity then abs(sls_price)*sls_quantity
			else sls_sales
		end sls_sales,
		sls_quantity,
		case when sls_price = 0 or sls_price is null then sls_sales/ nullif(sls_quantity, 0)
			else abs(sls_price)
		end sls_price 
		from bronze.crm_sales_details
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		--Silver.erp_cust_az12
		set @start_time = GETDATE()
		print '>> Truncating Table: silver.erp_cust_az12'
		truncate table silver.erp_cust_az12
		print '>> Inserting Data Into: silver.erp_cust_az12'
		insert into silver.erp_cust_az12(
			cid,
			birthdate,
			gender
		)

		select
		case when cid like 'NASA%' then substring(cid,4,len(cid))
			else cid
		end as cid,
		case when birthdate > GETDATE() then null
			else birthdate
		end as birthdate,
		case when upper(trim(gender)) in ('F','Female') then 'Female'
			when upper(trim(gender)) in ('M', 'Male') then 'Male'
			else 'N/A'
		end as gender
		from bronze.erp_cust_az12
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		--Silver.erp_loc_a101
		set @start_time = GETDATE()
		print '>> Truncating Table: silver.erp_loc_a101'
		truncate table silver.erp_loc_a101
		print '>> Inserting Data Into: silver.erp_loc_a101'
		insert into silver.erp_loc_a101(
			cid,
			country
		)
		select
		replace(cid, '-', '') cid,
		case when trim(country) = 'DE' then 'Germany'
			when trim(country) in ('US', 'USA') then 'United States'
			when trim(country) = '' or country is null then 'N/A'
			else trim(country)
		end as country 
		from bronze.erp_loc_a101
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		--Silver.erp_px_cat_g1v2
		set @start_time = GETDATE()
		print '>> Truncating Table: silver.erp_px_cat_g1v2'
		truncate table silver.erp_px_cat_g1v2
		print '>> Insert Data Into: silver.erp_px_cat_g1v2'
		insert into silver.erp_px_cat_g1v2(
			id,
			cat,
			subcat,
			maintenance
		)
		select *
		from bronze.erp_px_cat_g1v2
		set @end_time = GETDATE()
		print '>> Time Execute: ' + cast(datediff(second, @start_time, @end_time) as nvarchar) + ' seconds'
		print '----------------'

		set @batch_end_time = GETDATE()
		print '==================================='
		print 'Loading Silver Layer Successful. Total Time Execute: ' + cast(datediff(second, @batch_start_time, @batch_end_time) as nvarchar) + ' seconds'
		print '==================================='
	end try
	begin catch
		print '==================================='
		print 'Failed to loading silver layer. Error Message:' + error_message();
		print 'Failed to loading silver layer. Error Message:' + cast(error_number() as nvarchar);
		print 'Failed to loading silver layer. Error Message:' + cast(error_state() as nvarchar)
		print '==================================='
	end catch
end
