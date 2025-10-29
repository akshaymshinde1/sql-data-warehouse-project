--======================
--Advance Data Analytics
--======================
-- ----------------------------------
-- 1.Change Over Time
-- ----------------------------------
--Analyze Sales performance over time
-- Over year
SELECT
	YEAR(order_date) AS sales_year,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY sales_year

--over month
SELECT
	YEAR(order_date) AS sales_year,
	MONTH(order_date) AS sales_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY
	YEAR(order_date),
	MONTH(order_date)
ORDER BY
	sales_year,
	sales_month

-- to get the year and month in single column use DATETRUNK
-- this the best pracice
SELECT
	DATETRUNC(MONTH,order_date) AS sales_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH,order_date)
ORDER BY sales_month

--same result with FORMAT but in this the output date is string so ORDER will be incorrect
SELECT
FORMAT(order_date,'yyyy-MMM') AS sales_month,
SUM(sales_amount) AS total_sales,
COUNT(DISTINCT customer_key) AS total_customers,
SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY FORMAT(order_date,'yyyy-MMM')
ORDER BY FORMAT(order_date,'yyyy-MMM')


-- ----------------------------------
-- 2. Cumilative Analysis
-- ----------------------------------
-- Calculate total sales per month and the running total of sales over time
SELECT
	order_date,
	total_sales,
	SUM(total_sales) OVER(ORDER BY order_date) AS running_total_sales, -- running total of complete table
	--to get running total by YEAR
	--SUM(total_sales) OVER(PARTITION BY YEAR(ORDER_DATE) ORDER BY order_date) AS running_total_sales
	AVG(sales_avg) OVER(ORDER BY order_date) AS running_avg
FROM (
	SELECT
		DATETRUNC(MONTH, order_date) order_date,
		SUM(sales_amount) AS total_sales,
		AVG(price) AS sales_avg
	FROM gold.fact_sales
	WHERE order_date IS NOT NULL
	GROUP BY DATETRUNC(MONTH, order_date)
	)t


-- ----------------------------------
-- 3. Performance Analysis
-- ----------------------------------
-- Analyze the yearly performance of products by comparing each product's sales to both
-- it's avg sales performance and the previous year's sales.
SELECT
	product_name,
	year_of_sales,
	total_sales,
	AVG(total_sales) OVER(PARTITION BY product_name) AS avg_sales,  --if ORDER BY year_of_sales is not given it will calculate one avg for whole window
	LAG(total_sales,1) OVER(PARTITION BY product_name	ORDER BY year_of_sales) AS previous_year_sales
FROM (
	SELECT
		p.product_name,
		DATETRUNC(YEAR,s.order_date) AS year_of_sales,
		SUM(s.sales_amount) AS total_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY p.product_name, DATETRUNC(YEAR,s.order_date)
	--ORDER BY p.product_name, DATETRUNC(YEAR,s.order_date)  --orderby should not be included in subquery
 	)t

-- OR WITH CTE
WITH CTE AS (
	SELECT
		YEAR(s.order_date) AS year_of_sales,
		p.product_name,
		SUM(s.sales_amount) AS current_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
	WHERE s.order_date IS NOT NULL
	GROUP BY p.product_name, YEAR(s.order_date)
	)
SELECT
	year_of_sales,
	product_name,
	current_sales,
	AVG(current_sales) OVER (PARTITION BY product_name) AS avg_sales,
	--(current_sales) - (AVG(current_sales) OVER (PARTITION BY product_name)) AS avg_sales_difference,
	CASE WHEN ((current_sales) - (AVG(current_sales) OVER (PARTITION BY product_name))) > 0 THEN 'Above Avg'
		 WHEN ((current_sales) - (AVG(current_sales) OVER (PARTITION BY product_name))) < 0 THEN 'Below Avg'
		 ELSE 'Avg'
	END AS avg_change,
	LAG(current_sales) OVER(PARTITION BY product_name ORDER BY year_of_sales) AS previous_year_sales,
	--current_sales - ISNULL((LAG(current_sales) OVER(PARTITION BY product_name ORDER BY year_of_sales)),0) AS change_from_pre_year,
	CASE WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY year_of_sales)) > 0 THEN 'Increased'
		 WHEN (current_sales - LAG(current_sales) OVER(PARTITION BY product_name ORDER BY year_of_sales)) < 0 THEN 'Decreased'
		 ELSE 'This is First Year'
	END AS py_change
FROM CTE
ORDER BY product_name, year_of_sales


-- ----------------------------------
-- 4. Part-to-Whole
-- ----------------------------------
-- Which category contribute the most to overall sales
WITH CTE AS (
	SELECT
	p.category,
	SUM(s.sales_amount) AS category_total_sales
	FROM gold.fact_sales s
	LEFT JOIN gold.dim_products p ON s.product_key = p.product_key
	GROUP BY p.category
	--ORDER BY SUM(s.sales_amount) DESC
	)
SELECT
	category,
	category_total_sales,
	SUM(category_total_sales) OVER () AS total_sales,
	CONCAT(ROUND((CAST(category_total_sales AS FLOAT)/SUM(category_total_sales) OVER ()*100), 2), '%') AS percentage_sales
FROM CTE
ORDER BY category_total_sales DESC


-- ----------------------------------
-- 5. Data Segmentation
-- ----------------------------------
--Segment products into cost ranges and count how many products fall into each segment
WITH data_segmentation AS (
SELECT
product_id,
product_name,
cost,
CASE 
	WHEN cost >=0 AND cost <=800 THEN 'Low Price'
	WHEN cost > 800 AND cost <=1400 THEN 'Medium Price'
	ELSE 'High Price'
END AS cost_category
FROM gold.dim_products
)
--create temp_table as cte works for one query only
SELECT * INTO #data_segmentation FROM data_segmentation

--1
SELECT *,
COUNT(cost_category) OVER(PARTITION BY cost_category) AS category_count
FROM #data_segmentation;

--2
SELECT
cost_category,
count(*) category_count
FROM #data_segmentation
group by cost_category;

--Segment products into cost ranges and count how many products fall into each segment
WITH product_segment AS (
	SELECT
		product_key,
		product_name,
		cost,
		CASE
			WHEN cost < 100 THEN 'Below 100'
			WHEN cost BETWEEN 100 AND 500 THEN '100-500'
			WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
			ELSE 'Above 1000'
		END AS cost_range
	FROM gold.dim_products
	)
SELECT
cost_range,
COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range 
ORDER BY total_products DESC

--1.Group customers in three segments based on their spending nature
--i)VIP: at least 12 months of history and spending more than $5000
--ii)Regular: at least 12 months of history but spending $5000 or less
--iii)New: lifespan less than 12 months

SELECT
	C.customer_key,
	C.first_name,
	C.last_name,
	SUM(sales_amount) AS total_spending,
	--MIN(S.order_date) AS first_order,
	--MAX(S.order_date) AS last_order,
	DATEDIFF(MONTH, MIN(S.order_date), MAX(S.order_date)) AS lifespan,
	CASE WHEN SUM(sales_amount) > 5000 AND DATEDIFF(MONTH, MIN(S.order_date), MAX(S.order_date)) >= 12 THEN 'VIP'
		 WHEN SUM(sales_amount) <= 5000 AND DATEDIFF(MONTH, MIN(S.order_date), MAX(S.order_date)) >= 12 THEN 'Regular'
		 ELSE 'New'
	END AS cust_segment
FROM gold.fact_sales S
LEFT JOIN gold.dim_customers C ON S.customer_key = C.customer_key
GROUP BY 
	C.customer_key,
	C.first_name,
	C.last_name
ORDER BY C.customer_key
--this is showing correct results but while segmentation use CTE or SubQuery

WITH customer_spending AS(
	SELECT
		C.customer_key,
		SUM(F.sales_amount) AS total_spending,
		MIN(F.order_date) AS first_order,
		MAX(F.order_date) AS last_order,
		DATEDIFF(MONTH, MIN(F.order_date), MAX(F.order_date)) AS lifesapn
	FROM gold.fact_sales AS F
	LEFT JOIN gold.dim_customers AS C ON F.customer_key = C.customer_key
	GROUP BY C.customer_key
)
SELECT
	customer_key,
	total_spending,
	lifesapn,
	CASE WHEN total_spending > 5000 AND lifesapn >= 12 THEN 'VIP'
		 WHEN total_spending <=5000 AND lifesapn >=12 THEN 'Regular'
		 ELSE 'New'
	END AS cust_segment
FROM customer_spending

--2.Find total number of customers in each group
WITH customer_spending2 AS (
	SELECT
		C.customer_key,
		SUM(F.sales_amount) AS total_spending,
		MIN(F.order_date) AS first_order,
		MAX(F.order_date) AS last_order,
		DATEDIFF(MONTH, MIN(F.order_date), MAX(F.order_date)) AS lifesapn
	FROM gold.fact_sales AS F
	LEFT JOIN gold.dim_customers AS C ON F.customer_key = C.customer_key
	GROUP BY C.customer_key
)
SELECT
	cust_segment,
	COUNT(customer_key) AS total_customers
FROM(
	SELECT
		customer_key,
		CASE WHEN total_spending > 5000 AND lifesapn >= 12 THEN 'VIP'
			 WHEN total_spending <=5000 AND lifesapn >=12 THEN 'Regular'
			 ELSE 'New'
		END AS cust_segment
	FROM customer_spending2
	) T
GROUP BY cust_segment
ORDER BY total_customers

--SELECT * FROM gold.fact_sales ORDER BY customer_key
-- ----------------------------------
--
-- ----------------------------------