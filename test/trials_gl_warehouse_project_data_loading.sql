-- ----------------------------
-- 1. gold.dim_customers
-- ----------------------------
-- ----------------------------------------
-- join and check any duplicate data after join
-- ----------------------------------------
SELECT TOP 2 * FROM silver.erp_cust_az12 ca
SELECT TOP 2 * FROM silver.erp_loc_a101 cl


SELECT cst_id, COUNT(*) FROM(
SELECT
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
cl.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 cl ON ci.cst_key = cl.cid)T
GROUP BY cst_id
HAVING COUNT(*) > 1

-- ------------------------------------------------------
-- grndr integration
-- ------------------------------------------------------

SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT gen FROM silver.erp_cust_az12;

SELECT DISTINCT
ci.cst_gndr, --IN THIS CASE DISCUSS WITH BUSINESS EXPERT AND DECIDE WHICH IS MASTER TABLE. HERE IT IS CUST_INFO
ca.gen,
CASE WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr
	 ELSE COALESCE(ca.gen,'n/a')
END
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca ON ci.cst_key = ca.cid;

-- ----------------------------
-- 2.gold.dim_products
-- ----------------------------
SELECT TOP 2 * FROM silver.crm_prd_info;
SELECT TOP 2 * FROM silver.erp_px_cat_g1v2;

SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) AS product_key,
pn.prd_id AS product_id,
pn.cat_id AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 PC ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- WE DONT NEED HISTORICAL DATA

-- ----------------------------
-- 3.gold.dim_sales
-- ----------------------------
SELECT TOP 2 * FROM silver.crm_sales_details;
SELECT TOP 2 * FROM gold.dim_products;
SELECT TOP 2 * FROM gold.dim_customers;

SELECT
	sd.sls_ord_num AS order_number,
	pr.product_key,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS ship_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr ON sd.sls_prd_key = pr.product_number
left join gold.dim_customers cu on sd.sls_cust_id = cu.customer_id

-- ------------------------------
-- CHECKING VIEW JOINS
-- ------------------------------ 
SELECT * FROM gold.fact_sales fs
left join gold.dim_customers dc on fs.customer_key = dc.customer_key
where dc.customer_key is null

SELECT * FROM gold.fact_sales fs
left join gold.dim_products dc on fs.product_key = dc.product_key
where dc.product_key is null