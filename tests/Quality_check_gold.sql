--Kiểm tra Customer Key có unique hay không
select customer_key, count(*) as duplicated_key
from gold.dim_customers
group by customer_key
having count(*) > 1

-- Kiểm tra Product Key có unique hay không
select product_key, count(*) as duplicated_key
from gold.dim_products
group by product_key
having count(*) > 1

-- Kiểm tra kết nối Key giữa các bảng fact và dim có bị null không
select * 
from gold.fact_sales as fs
left join gold.dim_customers as dc
on fs.customer_key = dc .customer_key
left join gold.dim_products as dp
on fs.customer_key = dp.customer_key
where dc.customer_key is null
or dp.product_key is null
