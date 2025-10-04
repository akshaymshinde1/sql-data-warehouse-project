--========================================
--SL DATA LOADING TRIALS
--=========================================
-- -------------------
-- crm_cust_info
-- -------------------
-- -------------------------------------------------
-- 1. removing duplicates and nulls from primery key
-- -------------------------------------------------
--SELECT * FROM bronze.crm_cust_info

/*
SELECT cst_id, COUNT(*) FROM bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL
*/

/*
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info
--WHERE cst_id = 29466
*/

/*
SELECT * FROM(
SELECT *, 
ROW_NUMBER() OVER (PARTITION BY cst_id ORDER BY cst_create_date DESC) AS flag_last
FROM bronze.crm_cust_info)T
WHERE flag_last = 1
*/

-- ---------------------------------------------------------------------
-- 2. checking is there any leading or trailing spaces in string columns
-- ----------------------------------------------------------------------
/*
SELECT cst_firstname FROM bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)
*/

-- -------------------------------------
-- 3. checking distinct values in column
-- -------------------------------------
/*
select distinct(cst_marital_status) from bronze.crm_cust_info 
*/

-- -------------------
-- crm_prd_info
-- -------------------
-- -------------------------------------------------
-- 1. removing duplicates and nulls from primery key
-- -------------------------------------------------
/*
SELECT prd_id, COUNT(*) FROM bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 or prd_id IS NULL
-- there are no duplicates in prd_id
*/

-- ---------------------------------------------------------------
-- 2. Splitting prd_key in to two so that we will get category key
-- ---------------------------------------------------------------
/*
SELECT
prd_id,
prd_key,
REPLACE(LEFT(prd_key,5),'-','_') AS cat_id, -- HERE YOU CAN ALSO USE = SUBSTRING(prd_key,1,5)
SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
*/
--SELECT prd_key FROM bronze.crm_prd_info
--WHERE prd_key != TRIM(prd_key)

-- -------------------------------------------
-- 3. checking any unwanted spaces in prd_nm
-- -------------------------------------------
/*
SELECT * FROM bronze.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)
*/
-- So there is no need to use trim

-- ----------------------------------------------------------------------------------------
-- 4. prd_cost as per business needs check for negative or null cost and replace with zero
-- ----------------------------------------------------------------------------------------
/*
SELECT prd_cost FROM bronze.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL

SELECT ISNULL(prd_cost,0) AS prd_cost FROM bronze.crm_prd_info
*/

-- --------------------------------
-- 5. prd_line: give full name 
-- --------------------------------
/*
SELECT DISTINCT (prd_line) FROM bronze.crm_prd_info

SELECT
CASE WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
	 WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
	 WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'other Sales'
	 WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
	 ELSE 'n/a'
END AS prd_line
FROM bronze.crm_prd_info
*/

-- -------------------------------------------------------------
-- 6. start_dt and end_dt. Start date must smaller than end date
-- -------------------------------------------------------------
/*
SELECT prd_id, SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key, prd_start_dt, prd_end_dt FROM bronze.crm_prd_info
WHERE prd_start_dt > prd_end_dt 

SELECT prd_id,prd_key,prd_start_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 AS end_date
FROM bronze.crm_prd_info
*/

-- -------------------
-- crm_sales_details
-- -------------------
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
FROM bronze.crm_sales_details
WHERE sls_ord_num = 'SO72656'

-- -------------------------------------------------------------
-- 1. sla_ord_num: CHECK ANY DUPLICATES
-- -------------------------------------------------------------

SELECT sls_ord_num, COUNT(*) MULTI_ORD FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) >1
ORDER BY COUNT(*) DESC

SELECT COUNT(*) FROM(
SELECT sls_ord_num, COUNT(*) MULTI_ORD FROM bronze.crm_sales_details
GROUP BY sls_ord_num
HAVING COUNT(*) >1)T
--SO, 17991 sls_ord_num ARE DUPLICATE

SELECT sls_ord_num FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num)
--SO, THERE IS NO UNWANTED SPACES IN THE COLUMN

-- -------------------------------------------------------------------------------------------
-- 2. sls_prd_key,sls_cust_id : CHECK ALL THESE KEYS CAN BE CONNECTED WITH OTHER TABLES OR NOT
-- -------------------------------------------------------------------------------------------
SELECT
sls_prd_key
FROM bronze.crm_sales_details
WHERE sls_prd_key NOT IN (SELECT prd_key FROM silver.crm_prd_info)
--ALL KEYS ARE MATCHING

SELECT
sls_cust_id
FROM bronze.crm_sales_details
WHERE sls_cust_id NOT IN (SELECT cst_id FROM silver.crm_cust_info)
--ALL KEYS ARE MATCHING

-- ----------------------------------------------------------------------------------------------------
-- 3. sls_order_dt, sls_ship_dt, sls_due_dt: ALL THIS DATE COLUMNS HAVE INT DATA TYPE CHANGE IT TO DATE
-- ----------------------------------------------------------------------------------------------------
-- CHECK FOR INVALID DATES
SELECT sls_order_dt FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR
LEN(sls_order_dt) != 8 OR
sls_order_dt < 19000101 OR -- GET THIS DATE BOUNDRIES FROM BUSINESS EXPERT
sls_order_dt > 20510101
--THESE 0 OR INVALID NUMBERS CANT BE CONVERTED IN DATE TYPE

SELECT
sls_order_dt,
CASE
	WHEN sls_order_dt <= 0 OR LEN(sls_order_dt) != 8 THEN NULL
	ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE) -- IN SQL SERVER INT CAN NOT DIRECTLY CONVERTED INTO DATE 
END sls_order_dt
FROM bronze.crm_sales_details
--CHANGE ALL OTHER DT COLUMNS SAME WAY

--check sls_order_dt < sls_ship_dt < sls_due_dt this order of dates
SELECT
sls_order_dt,
sls_ship_dt,
sls_due_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt OR sls_ship_dt > sls_due_dt
--DATE ORDER CORRECT
-- ----------------------------------------------------------------------------------------------------
-- 4. sls_sales: SALES MUST NOT BE ZERO, NEGATIVE, NULL, NOT ALLOWED AND IT IS sls_quantity * sls_price
-- ----------------------------------------------------------------------------------------------------
SELECT
sls_sales AS old_sls_sales,
sls_quantity,
sls_price AS old_sls_price,
CASE WHEN sls_sales IS NULL OR sls_sales <=0 OR sls_sales != sls_quantity * ABS(sls_price) --COZ AT SOME ROWS PRICE IS NEGATIVE
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,
CASE WHEN sls_price IS NULL OR sls_price < 0 
	 THEN sls_sales / sls_quantity
	 ELSE sls_price
END AS sls_price
FROM bronze.crm_sales_details
WHERE 
sls_sales != sls_quantity * sls_price OR
sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL OR
sls_sales <= 0  OR sls_quantity <= 0  OR sls_price <= 0 
ORDER BY old_sls_sales,
sls_quantity,
old_sls_price
--QUALITY OF QUANTTITY IS GOOD AND PERFECT
--QUALITY OF SALES AND PRICE IS NOT GOOD
--DISCUSS THIS WITH BUSINESS EXPERT AND IMPROVE DATA QUALITY

-- -------------------
-- erp_loc_a101
-- -------------------
-- -------------------------------------------------------------
-- 1. cid : any cid with multiple countries
-- -------------------------------------------------------------
SELECT * FROM bronze.erp_loc_a101

SELECT cid, COUNT(*) FROM bronze.erp_loc_a101
GROUP BY cid
HAVING COUNT(*) > 1

SELECT * FROM bronze.erp_loc_a101
WHERE cid IS NULL

SELECT DISTINCT cntry AS OLD,
CASE WHEN TRIM(cntry) IN ('US','USA') THEN 'United States'
	 WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN cntry = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END AS cntry
FROM bronze.erp_loc_a101
ORDER BY cntry

SELECT * FROM bronze.erp_loc_a101
WHERE REPLACE(cid,'-','') NOT IN (SELECT cst_key FROM bronze.crm_cust_info)
-- the data is clean and good

-- -------------------
-- erp_cust_az12
-- -------------------
SELECT TOP 5 * FROM bronze.erp_cust_az12
SELECT TOP 5 * FROM bronze.crm_cust_info

--WE DONT HAVE ANY INFORMATION ABOUT 'NAS' IN CID SO REMOVE IT
SELECT
CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
	 ELSE cid
END AS cid,
bdate,
gen
FROM bronze.erp_cust_az12

SELECT DISTINCT gen2 FROM(
SELECT gen,
CASE WHEN UPPER(TRIM(gen)) = 'F' THEN 'Female'
	 WHEN UPPER(TRIM(gen)) = 'M' THEN 'Male'
	 WHEN gen = '' OR gen IS NULL THEN 'n/a'
	 ELSE gen
END AS gen2
FROM bronze.erp_cust_az12)T

--QUALITY CHECK OF NEW SILVER TABLE
SELECT  DISTINCT gen FROM silver.erp_cust_az12
select * from silver.erp_cust_az12

-- -------------------
-- erp_px_cat_g1v2
-- -------------------
SELECT
id,
cat,
subcat,
maintenance
FROM bronze.erp_px_cat_g1v2
--WHERE subcat != TRIM(subcat)

SELECT DISTINCT maintenance FROM bronze.erp_px_cat_g1v2

SELECT * FROM silver.erp_px_cat_g1v2
