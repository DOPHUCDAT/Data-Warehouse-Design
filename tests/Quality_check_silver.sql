-- Bảng bronze.crm_cust_info
-- Kiểm tra id có phải là duy nhất hay không 
select * 
from
(
	select *, ROW_NUMBER() over (partition by cst_id order by cst_create_date) as notunique_id
	from bronze.crm_cust_info
) as cleanned
where notunique_id != 1
-- Kiểm tra khoảng trắng tên

select cst_firstname
from bronze.crm_cust_info
where cst_firstname != trim(cst_firstname)

select cst_lastname
from bronze.crm_cust_info
where cst_lastname != trim(cst_lastname)

--Kiểm tra tên viết tắt

select cst_gender
from bronze.crm_cust_info
group by cst_gender

select cst_marital_status
from bronze.crm_cust_info
group by cst_marital_status

--Bảng bronze.crm_prd_info
select * from bronze.crm_prd_info
select * from bronze.crm_sales_details
-- Kiểm tra id có null hay nhiều hơn 1 bản ghi trùng lặp
select prd_id, count(*) from bronze.crm_prd_info
group by prd_id
having count(*) > 1 or prd_id is null 

-- Kiểm tra xem có bị thừa khoảng trắng không
select prd_nm from bronze.crm_prd_info
where prd_nm != trim(prd_nm)

-- Kiểm tra giá có bị âm hoặc null không
select prd_cost from bronze.crm_prd_info
where prd_cost < 0 or prd_cost is null

-- Kiểm tra các giá trị của loại dịch vụ
select distinct(prd_line) from bronze.crm_prd_info

--Kiểm tra logic của ngày tháng
select * from bronze.crm_prd_info
where prd_start_dt > prd_end_dt

--Bảng bronze.crm_sales_details
select * from bronze.crm_sales_details

--Kiểm tra logic ngày tháng có hợp lệ không
select nullif(sls_order_dt, 0) sls_order_dt from bronze.crm_sales_details
where sls_order_dt <= 0
or len(sls_order_dt) != 8
or sls_order_dt > 20300101
or sls_order_dt < 19000101

select nullif(sls_ship_dt, 0) sls_ship_dt from bronze.crm_sales_details
where sls_ship_dt <= 0
or len(sls_ship_dt) != 8
or sls_ship_dt > 20300101
or sls_ship_dt < 19000101

select nullif(sls_due_dt, 0) sls_due_dt from bronze.crm_sales_details
where sls_due_dt <= 0
or len(sls_due_dt) != 8
or sls_due_dt > 20300101
or sls_due_dt < 19000101

-- Kiểm tra logic của ngày order với ngày ship hàng và ngày nhận hàng
select sls_order_dt from bronze.crm_sales_details
where sls_order_dt > sls_ship_dt
or sls_order_dt > sls_due_dt

--Kiểm tra giá tiền, số lượng sản phẩm
select sls_sales,sls_quantity,sls_price from bronze.crm_sales_details
where sls_sales <= 0
or sls_quantity <= 0
or sls_price <= 0
or sls_sales is null
or sls_quantity is null
or sls_price is null
or sls_price * sls_quantity != sls_sales
order by sls_sales, sls_quantity, sls_price

--Bảng bronze.erp_cust_az12
--So sánh cid với cst_key có match với nhau không
select * from bronze.erp_cust_az12
select * from silver.crm_cust_info

--Xem có bao nhiêu bản ghi khác với cst_key
select * from bronze.erp_cust_az12
where cid like'NASA%'

--Kiểm tra logic năm sinh
select * from bronze.erp_cust_az12
where birthdate > GETDATE()

--Kiểm tra giới tính đã chuẩn hóa chưa
select gender,count(*) from bronze.erp_cust_az12
group by gender

--Bảng bronze.erp_loc_a101
select * from bronze.erp_loc_a101
select * from silver.crm_cust_info

select distinct(country) from bronze.erp_loc_a101

select * from bronze.erp_loc_a101
where cid is null 

--Bảng bronze.erp_px_cat_g1v2
select * from bronze.erp_px_cat_g1v2
select * from silver.crm_prd_info

select * from bronze.erp_px_cat_g1v2
where id is null
or cat is null
or subcat is null
or maintenance is null

select distinct maintenance from bronze.erp_px_cat_g1v2

select * from bronze.erp_px_cat_g1v2
where id not in (select cat_id from silver.crm_prd_info)

select * from bronze.erp_px_cat_g1v2
where cat != trim(cat) 
or subcat != trim(subcat) 
or maintenance != trim(maintenance) 
