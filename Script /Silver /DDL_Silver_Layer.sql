--create cust_info table
create table silver.crm_cust_info(
cst_id int,
cst_key nvarchar(50),
cst_firstname nvarchar(50),
cst_lastname nvarchar(50),
cst_marital_status nvarchar(10) ,
cst_gndr nvarchar(10),
cst_create_date date)
GO
--craete product table
create table silver.crm_prd_inf(
prd_id int,
prd_key nvarchar(50),
prd_nm nvarchar(50),
prd_cost int,
prd_line nvarchar(10) ,
prd_start_dt date,
prd_end_dt date)
GO
--create sales table 
create table silver.crm_Sales_details(
sls_ord_num nvarchar(50),
sls_prd_key nvarchar(50),
 sls_cust_id  INT,
sls_order_dt INT,
sls_ship_dt  INT,
sls_due_dt   INT,
sls_sales    INT,
sls_quantity INT,
sls_price    INT)

GO
--location table 
CREATE TABLE silver.erp_loc_a101 (
cid    NVARCHAR(50),
cntry  NVARCHAR(50)
)
GO
--create category table 
CREATE TABLE silver.erp_px_cat_g1v2 (
id           NVARCHAR(50),
cat          NVARCHAR(50),
subcat       NVARCHAR(50),
maintenance  NVARCHAR(50)
)
GO

--
CREATE TABLE silver.erp_cust_az12 (
    cid    NVARCHAR(50),
    bdate  DATE,
    gen    NVARCHAR(50)
)
GO
