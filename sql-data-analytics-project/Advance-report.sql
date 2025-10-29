
/*
=================================================================================
Customer Report
=================================================================================
Purpose:
	-This report consolidates key customer metrics and behaviors.

Heigligths:
	1. Gather essentials fields such as name, age, transaction details.
	2.Segmrnt customers into categories (VIP, Regular, New) and age group
	3. Aggregate customer level metrics:
		-total orders
		-total sales
		-total quantity purchased
		-total products
		-lifespan
	4. Calculate valuable KPI's:
		-recency (month since last order)
		-avg order value
		-avg monthly spend
=====================================================================================
*/

CREATE VIEW gold.report_customers AS 
WITH base_query AS(
/*--------------------------------------------------------------------
1) Base query: Retrives core columns from tables
--------------------------------------------------------------------*/
	SELECT
		F.order_number,
		F.product_key,
		F.order_date,
		F.sales_amount,
		F.quantity,
		C.customer_key,
		C.customer_number,
		CONCAT(C.first_name, ' ', C.last_name) AS customer_name,
		DATEDIFF(year, C.birth_date, getdate()) AS age
	FROM gold.fact_sales F
	LEFT JOIN gold.dim_customers C ON F.customer_key = C.customer_key
	WHERE order_date IS NOT NULL
	),
customer_aggregation AS (
/*--------------------------------------------------------------------
2) Customer Agregation: Summerizes key metrics at the customer level
--------------------------------------------------------------------*/
	SELECT
		customer_key,
		customer_number,
		customer_name,
		age,
		COUNT(DISTINCT order_number) AS total_orders,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		COUNT(DISTINCT product_key) AS total_products,
		MAX(order_date) AS last_order_date,
		DATEDIFF(month, MIN(order_date), MAX(order_date)) AS lifespan
	FROM base_query
	GROUP BY
		customer_key,
		customer_number,
		customer_name,
		age
	)
/*--------------------------------------------------------------------
3) Final report table
--------------------------------------------------------------------*/
SELECT
	--1
	customer_key,
	customer_number,
	customer_name,
	age,
	--2
	CASE
		WHEN age < 20 THEN 'Under 20'
		WHEN age BETWEEN 20 AND 29 THEN '20-29'
		WHEN age BETWEEN 30 AND 39 THEN '30-39'
		WHEN age BETWEEN 40 AND 49 THEN '40-49'
		ELSE '50 and Above'
	END AS age_group,
	CASE
		WHEN lifespan >=12 AND total_sales > 5000 THEN 'VIP'
		WHEN lifespan >=12 AND total_sales <=5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment,
	--4
	last_order_date,
	DATEDIFF(month, last_order_date, GETDATE()) AS recency,
	--3
	total_orders,
	total_sales,
	--AVG(total_sales) OVER(PARTITION BY customer_key) AS average_order_value, --this is incorrect as we have each customer key once only so it will show same as sales value
	total_quantity,
	total_products,
	lifespan,
	--4
	CASE 
		WHEN total_sales = 0 THEN 0
		ELSE total_sales/total_orders 
	END AS avg_order_value, --to make sure there will be no zero in num or D case statement is used
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_sales/lifespan 
	END AS avg_monthly_spend --as for new customer the lifespan will be zero used case statement
FROM customer_aggregation
/*STEPS IN CREATING THIS VIEW--------------------------------------------------------------------------------
--SELECT * FROM gold.report_customers
1.first create cte
2.crearte seperate cte for aggregation 
3.create final table
4.create VIEW as it is easy to run and view in one line and also to connect with dashboard
5.also from view one can directly find some insights like
	SELECT
		age_group,
		COUNT(customer_key) AS total_customers,
		SUM(total_sales) AS total_sales
	FROM gold.report_customers
	GROUP BY age_group
	ORDER BY total_sales DESC
-------------------------------------------------------------------------------------------------------------*/


/*
=================================================================================
Product Report
=================================================================================
Purpose:
	-This report consolidates key Product metrics and behaviors.

Heigligths:
	1. Gather essentials fields such as product name, category, subcategory and.
	2.Segmrnt products by revenue to identify HIgh-Performers, Mid-Range or Low-Performer
	3. Aggregate Product-level metrics:
		-total orders
		-total sales
		-total quantity sold
		-total customers (unique)
		-lifespan (in months)
	4. Calculate valuable KPI's:
		-recency (month since last sale)
		-avg order revenue (AOR)
		-avg monthly revenue
=====================================================================================
*/

CREATE VIEW gold.report_product AS
WITH base_query AS (
	SELECT
		F.order_number,
		F.order_date,
		F.customer_key,
		F.sales_amount,
		F.quantity,
		P.product_key,
		P.product_name,
		P.category,
		P.subcategory,
		P.cost
	FROM gold.fact_sales F
	LEFT JOIN gold.dim_products P ON F.product_key = P.product_key
	WHERE order_date IS NOT NULL
	),
product_aggregation AS(
	SELECT
		product_key,
		product_name,
		category,
		subcategory,
		cost,
		DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
		MAX(order_date) AS last_sale_date,
		COUNT(DISTINCT order_number) AS total_orders,
		COUNT(DISTINCT customer_key) AS total_customers,
		SUM(sales_amount) AS total_sales,
		SUM(quantity) AS total_quantity,
		ROUND(AVG(CAST(sales_amount AS FLOAT) / NULLIF(quantity, 0)), 1) AS avg_selling_price
	FROM base_query
	GROUP BY
		product_key,
		product_name,
		category,
		subcategory,
		cost
	)
SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'High-Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low-Performer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_customers,
	avg_selling_price,
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_sales / total_orders
	END AS avg_order_revenue,
	CASE
		WHEN lifespan = 0 THEN 0
		ELSE total_sales/lifespan
	END AS avg_monthly_revenue
FROM product_aggregation