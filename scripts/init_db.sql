create database DataWarehouse;
use DataWareHouse;

--Không nên chạy script này nếu chưa có backup vì sẽ gây ra mất dữ liệu
if exists (select 1 from sys.databases where name = 'DataWareHouse')
begin
	alter database DataWarehouse set single_user with rollback immediate;
	drop database DataWarehouse;
end
go

Create schema bronze;
go
Create schema silver;
go
Create schema gold
