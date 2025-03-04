-- View Dim Customers
if OBJECT_ID('gold.dim_customers', 'V') is not null
   drop view 'gold.dim_customers'
go
create or alter view gold.dim_customers as
select
    row_number() over (order by cst_id) as customer_key,
    ci.cst_id as customer_ID,
    ci.cst_key as customer_number,
    ci.cst_firstname as first_name,
    ci.cst_lastname as last_name,
    case when ci.cst_gender != 'N/A' then ci.cst_gender
        else coalesce(ca.gender, 'N/A')
    end as gender,
    ci.cst_marital_status as marital_status,
    ci.cst_create_date as create_date,
    ca.birthdate,
    la.country
from silver.crm_cust_info as ci
left join silver.erp_cust_az12 as ca
    on ci.cst_key = ca.cid
left join silver.erp_loc_a101 as la
    on ci.cst_key = la.cid
go

--View Dim Products
if OBJECT_ID('gold.dim_products', 'V') is not null
   drop view 'gold.dim_products'
go
create or alter view gold.dim_products as
select 
    row_number() over (order by prd_start_dt, prd_key) as product_key,
    pi.prd_id as product_ID,
    pi.prd_key as product_number,
    pi.prd_nm as product_name,
    pi.cat_id as category_ID,
    pcg.cat as category,
    pcg.subcat as subcategory,
    pcg.maintenance,
    pi.prd_cost as product_cost,
    pi.prd_line as product_line,
    pi.prd_start_dt as product_start_date
from silver.crm_prd_info as pi
left join silver.erp_px_cat_g1v2 as pcg
    on pi.cat_id = pcg.id
where pi.prd_end_dt is null
go

-- View Fact Sales
if OBJECT_ID('gold.fact_sales', 'V') is not null
   drop view 'gold.fact_sales'
go
create or alter view gold.fact_sales as
select
    sd.sls_ord_num as order_number,
    dp.product_key,
    dc.customer_key,
    sd.sls_order_dt as order_date,
    sd.sls_ship_dt as ship_date,
    sd.sls_due_dt as due_date,
    sd.sls_sales as sales_amount,
    sd.sls_quantity as quantity,
    sd.sls_price as price
from silver.crm_sales_details as sd
left join gold.dim_customers as dc
    on sd.sls_cust_id = dc.customer_ID
left join gold.dim_products as dp
    on sd.sls_prd_key = dp.product_number
go