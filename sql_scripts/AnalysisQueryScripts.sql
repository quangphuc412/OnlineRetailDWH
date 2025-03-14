-- Check num of table records
SELECT COUNT(*) AS count_product FROM dim_product dp
--SELECT COUNT(*) AS count_customer FROM dim_customer dc
--SELECT COUNT(*) AS count_datetime FROM dim_date dd
--SELECT COUNT(*) AS count_invoices_fact FROM fact_invoices fi

-- Check unique of Primary key
-- dim_product
SELECT product_id, COUNT(*) FROM dim_product dp GROUP BY product_id HAVING COUNT(*) > 1;
-- dim_customer 
SELECT customer_id, COUNT(*) FROM dim_customer dc GROUP BY customer_id HAVING COUNT(*) > 1;
--dim_date
SELECT datetime_id, COUNT(*) FROM dim_date dd GROUP BY datetime_id HAVING COUNT(*) > 1;

-- Kiểm tra tính toàn vẹn tham chiếu (ví dụ: invoices_fact và product_dim)
SELECT fi.product_id
FROM fact_invoices fi 
LEFT JOIN dim_product dp ON dp.product_id = fi.product_id
WHERE dp.product_id IS NULL;

-- Basic analyst
-- Total sales
SELECT 
	SUM(total) AS total_revenue
	, COUNT(*) AS number_of_invoices 
FROM fact_invoices fi;

-- Total sales by Product
SELECT dp.description, SUM(fi.total) AS revenue
FROM fact_invoices fi
JOIN dim_product dp ON fi.product_id = dp.product_id
GROUP BY dp.description
ORDER BY revenue DESC
LIMIT 10;

-- Total sales by Country
SELECT dc.country, SUM(fi.total) AS revenue
FROM fact_invoices fi 
JOIN dim_customer dc ON fi.customer_id = dc.customer_id
GROUP BY dc.country
ORDER BY revenue DESC
LIMIT 10;

-- Top 5 product sold by each country
WITH product_sales AS(
	SELECT
		dc.country
		, fi.product_id
		, dp.description 
		, SUM(fi.quantity) AS number_of_product_sold
		, DENSE_RANK() OVER (PARTITION BY dc.country ORDER BY SUM(fi.quantity) DESC) AS rank
	FROM fact_invoices fi 
	JOIN dim_product dp ON dp.product_id = fi.product_id
	JOIN dim_customer dc ON dc.customer_id = fi.customer_id
	GROUP BY
		dc.country
		, fi.product_id
		, dp.description
)
SELECT
    *
FROM product_sales
WHERE rank <= 5
ORDER BY country, rank;

-- Total sales by Month
SELECT dd."year", dd."month", SUM(fi.total) AS revenue
FROM fact_invoices fi 
JOIN dim_date dd ON fi.datetime_id = dd.datetime_id
GROUP BY dd."year" , dd."month" 
ORDER BY dd."year" , dd."month";

-- Tolal sales by day of week
SELECT 
	dd.day_name
	, SUM(fi.total) AS total
	, SUM(fi.quantity) AS number_of_product_sold
	, COUNT(DISTINCT fi.invoice_id) AS number_of_invoice
FROM fact_invoices fi 
JOIN dim_date dd ON fi.datetime_id = dd.datetime_id
GROUP BY dd.day_name

--Total sales by hours
SELECT 
	dd."hour" 
	, SUM(fi.total) AS total
	, SUM(fi.quantity) AS number_of_product_sold
	, COUNT(DISTINCT fi.invoice_id) AS number_of_invoice
FROM fact_invoices fi 
JOIN dim_date dd ON fi.datetime_id = dd.datetime_id
GROUP BY dd."hour" 
ORDER BY dd."hour" 

-- Top Products by Quantity Sold
SELECT fi.product_id, dp.description, SUM(quantity) AS quantity 
FROM fact_invoices fi 
JOIN dim_product dp ON dp.product_id = fi.product_id 
GROUP BY fi.product_id, dp.description
ORDER BY quantity DESC
LIMIT 10;

-- Top Products by Number of Customers
SELECT fi.product_id, dp.description, COUNT(DISTINCT customer_id) AS number_of_customer_bought
FROM fact_invoices fi 
JOIN dim_product dp ON dp.product_id = fi.product_id 
GROUP BY fi.product_id, dp.description
ORDER BY COUNT(DISTINCT customer_id) DESC 
LIMIT 10;

-- RFM analysis
WITH rfm_base AS(
	SELECT 
		fi.customer_id
		, MAX(dd.datetime) AS last_purchase_date
		, COUNT(DISTINCT fi.invoice_id) AS frequency
		, SUM(fi.total) AS monetary
	FROM fact_invoices fi
	JOIN dim_customer dc ON dc.customer_id = fi.customer_id
	JOIN dim_date dd ON dd.datetime_id = fi.datetime_id 
	GROUP BY fi.customer_id
), 
max_date AS(
	SELECT MAX(DISTINCT dd.datetime) AS max_date
	FROM fact_invoices fi
	JOIN dim_date dd ON dd.datetime_id = fi.datetime_id
),
rfm_calculated AS(
	SELECT
		*
		, EXTRACT(DAY FROM ((SELECT max_date FROM max_date) - last_purchase_date)::interval) AS recency
		, NTILE(5) OVER (ORDER BY EXTRACT (DAY FROM ((SELECT max_date FROM max_date) - last_purchase_date)::interval) DESC) AS r_score
	    , NTILE(5) OVER (ORDER BY frequency) AS f_score
	    , NTILE(5) OVER (ORDER BY monetary) AS m_score
	FROM rfm_base
),
rfm_segment AS (
	SELECT
		customer_id
		, recency
		, frequency
		, monetary
		, r_score
		, f_score
		, m_score
		, (CAST(r_score AS VARCHAR) || CAST(f_score AS VARCHAR) || CAST(m_score AS VARCHAR)) AS rfm_segment
	FROM rfm_calculated
	ORDER BY rfm_segment
)
SELECT * FROM rfm_segment
--SELECT 
--	rfm_segment
--	, count(*) AS num
--FROM rfm_segment
--GROUP BY rfm_segment
--ORDER BY count(*) DESC