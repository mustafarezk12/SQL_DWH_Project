/*
-------------------------------------
Create DB & Schema 
-------------------------------------
Script Purpose: This script creates a new DB 
named 'DataWarehouse'. Additionally , the script
sets up three schemas within the database: 'bronze'
,'silver' and 'gold'

*/
use master;
GO
--Drope & recreate the 'DataWarehouse'
if exists (select 1 from sys.databases where name = 'DataWarehouse')
Begin
alter database DataWarehouse set SINGLE_USER With ROLLBACK IMMEDIATE;
DROP database DataWarehouse
end
go
--Create DB
Create Database DataWarehouse 

-- Create Schema
Create schema bronze;
Create schema silver;
Create schema gold;