/*
===============================================================================
Quality Checks
===============================================================================
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, 
    and standardization across the 'silver' layer. It includes checks for:
    - Null or duplicate primary keys.
    - Unwanted spaces in string fields.
    - Data standardization and consistency.
    - Invalid date ranges and orders.
    - Data consistency between related fields.

Usage Notes:
    - Run these checks after data loading Silver Layer.
    - Investigate and resolve any discrepancies found during the checks.
===============================================================================
*/

-- ====================================================================
-- Checking 'silver.crm_cust_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT cst_id, COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectation: no result
-- Repeat for string features
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname);

-- Check for data consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

-- ====================================================================
-- Checking 'silver.crm_prd_info'
-- ====================================================================
-- Check for NULLs or Duplicates in Primary Key
-- Expectation: No Results
SELECT prd_id, COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectation: no result
-- Repeat for string features
SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for nulls or negative numbers
-- Expectation: No results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check for data consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for invalid date orders (Start date > End Date)
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ====================================================================
-- Checking 'silver.crm_sales_details'
-- ====================================================================
-- Check for unwanted spaces
-- Expectation: no result
-- Repeat for string features
SELECT
    sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
    sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check quality of product key from silver product info
-- Repeat for customer id
-- Expectation: no result
SELECT
    sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
    sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info);

-- Check for invalid dates
-- Repeat for all dates
SELECT 
    NULLIF(sls_order_dt,0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 
    OR LEN(sls_order_dt) != 8
    OR sls_order_dt > 20500101
    OR sls_order_dt < 19000101;

-- Check for invalid dates orders (Order date > Shipping/Due dates)
-- Expectation: No Results
SELECT *
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt
    OR sls_order_dt > sls_due_dt;

-- Check for data consistency in sales, price, and quantity
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
    OR sls_sales IS NULL 
    OR sls_quantity IS NULL 
    OR sls_price IS NULL
    OR sls_sales <= 0 
    OR sls_quantity <= 0 
    OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;

-- ====================================================================
-- Checking 'silver.erp_cust_az12'
-- ====================================================================
-- Check invalid date, data type, or out of range dates
-- Expectation: Birthdates between 1924-01-01 and Today
SELECT bdate
FROM silver.erp_cust_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();

-- Check for data consistency
SELECT DISTINCT gen
FROM silver.erp_cust_az12;

-- ====================================================================
-- Checking 'silver.erp_loc_a101'
-- ====================================================================
-- Check for data consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101;

-- ====================================================================
-- Checking 'silver.erp_px_cat_g1v2'
-- ====================================================================
-- Check for unwanted spaces
-- Expectation: No Results
SELECT * 
FROM silver.erp_px_cat_g1v2
WHERE cat != TRIM(cat) 
    OR subcat != TRIM(subcat) 
    OR maintenance != TRIM(maintenance);

-- Check for data consistency
SELECT DISTINCT cat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT subcat
FROM silver.erp_px_cat_g1v2;

SELECT DISTINCT maintenance
FROM silver.erp_px_cat_g1v2;
