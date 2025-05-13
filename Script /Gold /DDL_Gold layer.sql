/*
===============================================================================
DDL Script: Create Gold Views
===============================================================================
Script Purpose:
    This script creates views for the Gold layer in the data warehouse. 
    The Gold layer represents the final dimension and fact tables (Star Schema)

    Each view performs transformations and combines data from the Silver layer 
    to produce a clean, enriched, and business-ready dataset.

Usage:
    - These views can be queried directly for analytics and reporting.
===============================================================================
*/

-- =============================================================================
-- Create Dimension: gold.dim_customers
-- =============================================================================
IF OBJECT_ID('gold.dim_customers', 'V') IS NOT NULL
    DROP VIEW gold.dim_customers;
GO
create view  gold.dim_customers
as
select 
	ROW_NUMBER() over (order by cst_id) as customer_SK,
	c.cst_id as customer_id,
	c.cst_key as customer_number,
	c.cst_firstname as first_name,
	c.cst_lastname as last_name,
	l.cntry as country,
	c.cst_marital_status as marital_status,
	case when c.cst_gndr != 'n/a' then c.cst_gndr --crm is the master for gender info 
	else coalesce(e.gen, 'n/a')
	end  as gender,
	e.bdate as birth_date,
	c.cst_create_date as create_date
from silver.crm_cust_info c
left join silver.erp_cust_az12 e
on c.cst_key = e.cid
left join silver.erp_loc_a101 l
on l.cid = e.cid
--)
-- =============================================================================
-- Create Dimension: gold.dim_products
-- =============================================================================
IF OBJECT_ID('gold.dim_products', 'V') IS NOT NULL
    DROP VIEW gold.dim_products;
GO

create view gold.dim_products as 
select
    ROW_NUMBER() over (order by p.prd_key , p.prd_start_dt) as product_SK,
	p.prd_id,
	p.prd_key,
	p.prd_nm,
	p.cat_id,
	pc.subcat,
	pc.maintenance,
	p.prd_cost,
	p.prd_line,
	p.prd_start_dt
from silver.crm_prd_info p
left join silver.erp_px_cat_g1v2 pc
on p.cat_id = pc.id

select * from silver.crm_prd_info
select * from silver.erp_px_cat_g1v2


-- =============================================================================
-- Create Fact Table: gold.fact_sales
-- =============================================================================
IF OBJECT_ID('gold.fact_sales', 'V') IS NOT NULL
    DROP VIEW gold.fact_sales;
GO
--tranactions-->> Facts
-- use the dims surrgout keys instead of ids to easily connectfacts with dims
CREATE VIEW gold.fact_sales AS
SELECT
    sd.sls_ord_num  AS order_number,
    pr.product_SK  AS product_key,
    cu.customer_SK AS customer_key,
    sd.sls_order_dt AS order_date,
    sd.sls_ship_dt  AS shipping_date,
    sd.sls_due_dt   AS due_date,
    sd.sls_sales    AS sales_amount,
    sd.sls_quantity AS quantity,
    sd.sls_price    AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
    ON sd.sls_prd_key = pr.product_SK
LEFT JOIN gold.dim_customers cu
    ON sd.sls_cust_id = cu.customer_id;
