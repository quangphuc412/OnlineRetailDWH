/*
===============================================================================
Customer Report
===============================================================================
Purpose:
    - This report consolidates key customer metrics and behaviors

Highlights:
    1. Gathers essential fields such as customer_id, country and transaction details.
	2. Segments customers into categories (VIP, Regular, New).
    3. Aggregates customer-level metrics:
	   - total orders
	   - total sales
	   - total quantity purchased
	   - total products
	   - lifespan (in months)
    4. Calculates valuable KPIs:
	    - recency (months since last order)
		- average order value
		- average monthly spend
===============================================================================
*/

-- =============================================================================
-- Create Report: gold.report_customers
-- =============================================================================
DROP VIEW IF EXISTS report_customers;
GO

CREATE VIEW report_customers AS

WITH base_query AS(
/*---------------------------------------------------------------------------
1) Base Query: Retrieves core columns from tables
---------------------------------------------------------------------------*/
SELECT
	fi.invoice_id,
	dd.datetime,
	fi.product_id,
	fi.customer_id,
	dc.country,
	fi.quantity,
	fi.total 
FROM fact_invoices fi 
LEFT JOIN dim_customer dc 
	ON dc.customer_id = fi.customer_id 
LEFT JOIN dim_date dd
	ON dd.datetime_id = fi.datetime_id 
),
customer_aggregation AS (
/*---------------------------------------------------------------------------
2) Customer Aggregations: Summarizes key metrics at the customer level
---------------------------------------------------------------------------*/
SELECT 
	customer_id,
	country,
	COUNT(DISTINCT invoice_id) AS total_orders,
	SUM(total) AS total_sales,
	SUM(quantity) AS total_quantity,
	COUNT(DISTINCT product_id) AS total_products,
	MIN(datetime) AS first_order_date,
	MAX(datetime) AS last_order_date,
	EXTRACT(MONTH FROM JUSTIFY_INTERVAL(MAX(datetime) - MIN(datetime))) + 
		EXTRACT(YEAR FROM JUSTIFY_INTERVAL(MAX(datetime) - MIN(datetime))) * 12 AS lifespan
FROM base_query
GROUP BY 
	customer_id,
	country
)
SELECT
	customer_id,
	country,
	CASE 
	    WHEN lifespan >= 9 AND total_sales > 10000 THEN 'VIP'
	    WHEN lifespan >= 9 AND total_sales <= 10000 THEN 'Regular'
	    ELSE 'New'
	END AS customer_segment,
	first_order_date,
	last_order_date,
	EXTRACT(DAY FROM (SELECT MAX(datetime) FROM dim_date) - last_order_date) AS recency,
	lifespan,
	total_orders,
	total_sales,
	total_quantity,
	total_products,
	-- Compuate average order value (AVO)
	CASE WHEN total_sales = 0 THEN 0
		 ELSE total_sales / total_orders
	END AS avg_order_value,
	-- Compuate average monthly spend
	CASE WHEN lifespan = 0 THEN total_sales
	     ELSE total_sales / lifespan
	END AS avg_monthly_spend
FROM customer_aggregation