--Data Preparation--
CREATE TABLE IF NOT EXISTS products (
	product_id VARCHAR(50) NOT NULL,
    product_category_name VARCHAR(50),
    product_name_lenght INT,
	product_description_lenght INT,
	product_photos_qty INT,
	product_weight_g INT,
	product_length_cm INT,
	product_height_cm INT,
	product_width_cm INT,
	CONSTRAINT products_pkey PRIMARY KEY (product_id)
);
COPY products
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\product_dataset.csv'
DELIMITER ';'
CSV HEADER;

CREATE TABLE IF NOT EXISTS payments (
	order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
	payment_installments INT,
	payment_value DOUBLE PRECISION
);
COPY payments
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\order_payments_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS sellers (
	seller_id VARCHAR(50) NOT NULL,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(50),
	seller_state VARCHAR(50),
	CONSTRAINT sellers_pkey PRIMARY KEY (seller_id)
);
COPY sellers
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\sellers_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS order_items (
	order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
	seller_id VARCHAR(50),
	shipping_limit_date DATE,
	price DOUBLE PRECISION,
	freight_value DOUBLE PRECISION
);
COPY order_items
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\order_items_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS orders (
	order_id VARCHAR(50) NOT NULL,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
	order_purchase_timestamp DATE,
	order_approved_at DATE,
	order_delivered_carrier_date DATE,
	order_delivered_customer_date DATE,
	order_estimated_delivery_date DATE,
	CONSTRAINT orders_pkey PRIMARY KEY (order_id)
);
COPY orders
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\orders_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS reviews (
	review_id VARCHAR(250),
    order_id VARCHAR(250),
    review_score VARCHAR(250),
	review_comment_title VARCHAR(250),
	review_comment_message VARCHAR(250),
	review_creation_date VARCHAR(250),
	review_answer_timestamp DATE
);
COPY reviews
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\order_reviews_dataset.csv'
DELIMITER ';'
CSV HEADER;

CREATE TABLE IF NOT EXISTS geolocations (
	geolocation_zip_code_prefix INT,
    geolocation_lat DOUBLE PRECISION,
    geolocation_lng DOUBLE PRECISION,
	geolocation_city VARCHAR(50),
	geolocation_state VARCHAR(50)
);
COPY geolocations
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\geolocation_dataset.csv'
DELIMITER ','
CSV HEADER;

CREATE TABLE IF NOT EXISTS customers (
	customer_id VARCHAR(50) NOT NULL,
    customer_unique_id VARCHAR(50),
    customer_zip_code_prefix INT,
	customer_city VARCHAR(50),
	customer_state VARCHAR(50),
	CONSTRAINT customers_pkey PRIMARY KEY (customer_id)
);
COPY customers
FROM 'E:\Course\Rakamin\JAP\Analyzing eCommerce Business Performance with SQL\customers_dataset.csv'
DELIMITER ','
CSV HEADER;

ALTER TABLE IF EXISTS payments
	ADD CONSTRAINT payments_order_id_fkey FOREIGN KEY (order_id)
	REFERENCES orders (order_id);

ALTER TABLE IF EXISTS order_items 
	ADD CONSTRAINT order_items_order_id_fkey FOREIGN KEY (order_id)
	REFERENCES orders (order_id);
	
ALTER TABLE IF EXISTS order_items
	ADD CONSTRAINT order_items_product_id_fkey FOREIGN KEY (product_id)
	REFERENCES products (product_id);
	
ALTER TABLE IF EXISTS order_items
	ADD CONSTRAINT order_items_seller_id_fkey FOREIGN KEY (seller_id)
	REFERENCES sellers (seller_id);
	
ALTER TABLE IF EXISTS orders
	ADD CONSTRAINT orders_customer_id_fkey FOREIGN KEY (customer_id)
	REFERENCES customers (customer_id);

/*One of the metrics used to measure eCommerce business performance is customer activity interacting
within the eCommerce platform.*/

--Annual Customer Activity Growth Analysis--
WITH average_customer_active AS (
	SELECT years,
	       ROUND(AVG(customer_active)) AS average_customer_active_per_month
    FROM (
		SELECT DATE_PART('YEAR', t1.order_purchase_timestamp) AS years,
	           DATE_PART('MONTH', t1.order_purchase_timestamp) AS months,
 	           COUNT(t2.customer_unique_id) AS customer_active
		FROM orders AS t1
		LEFT JOIN customers AS t2 ON t2.customer_id = t1.customer_id
		WHERE order_status != 'canceled'
		GROUP BY 1,2
	 	) AS active_customer_per_month
	GROUP BY 1
),
new_customer AS (
	SELECT DATE_PART('YEAR', order_purchase_timestamp) AS years,
	       COUNT(customer_unique_id) AS number_of_new_customer
	FROM (
		SELECT t2.customer_unique_id,
	           MIN(t1.order_purchase_timestamp) AS order_purchase_timestamp
    	FROM orders AS t1
		LEFT JOIN customers AS t2 ON t2.customer_id = t1.customer_id
		WHERE order_status != 'canceled'
    	GROUP BY 1
		) AS purchase_time
	GROUP BY 1
	ORDER BY 1
),
repeat_orders AS (
	SELECT DATE_PART('YEAR', order_purchase_timestamp) AS years,
	       COUNT(customer_unique_id) as number_of_customer_repeat_order
	FROM (	
		SELECT t1.order_purchase_timestamp,
	           t2.customer_unique_id,
	           COUNT(t1.order_id) AS number_of_order
		FROM orders AS t1
		LEFT JOIN customers AS t2 ON t2.customer_id = t1.customer_id
		WHERE order_status != 'cancelled'
		GROUP BY 1,2
		HAVING COUNT(t1.order_id) > 1
		) AS repeat_order
	GROUP BY 1
	ORDER BY 1
),
average_number_order_customer AS (
	SELECT DATE_PART('YEAR', order_purchase_timestamp) AS years,
           ROUND(AVG(number_of_order)) AS average_number_of_order_per_customer
	FROM (
		SELECT t1.order_purchase_timestamp,
	       	   t2.customer_unique_id,
	           COUNT(t1.order_id) AS number_of_order
		FROM orders AS t1
		LEFT JOIN customers AS t2 ON t2.customer_id = t1.customer_id
		WHERE order_status != 'cancelled'
		GROUP BY 1,2
		) AS number_order
	GROUP BY 1
	ORDER BY 1
)
SELECT cte1.years,
       cte1.average_customer_active_per_month,
	   cte2.number_of_new_customer,
	   cte3.number_of_customer_repeat_order,
	   cte4.average_number_of_order_per_customer
FROM average_customer_active AS cte1
JOIN new_customer AS cte2 ON cte2.years = cte1.years
JOIN repeat_orders AS cte3 ON cte3.years = cte1.years
JOIN average_number_order_customer AS cte4 ON cte4.years = cte1.years;

/* The performance of the eCommerce business is of course very closely related to the products available
inside it.*/

--Annual Product Category Quality Analysis--
CREATE TEMP TABLE total_revenue AS
	SELECT DATE_PART('YEAR', order_purchase_timestamp) AS years,
	   	   ROUND(SUM(price + freight_value)) AS revenue
	FROM (
		SELECT t1.order_id,
               t1.order_purchase_timestamp,
	           t2.price,
	           t2.freight_value
		FROM orders AS t1
		LEFT JOIN order_items AS t2 ON t2.order_id = t1.order_id
		WHERE order_status NOT IN ('unavailable','invoiced','canceled')
		) as rev
	GROUP BY 1;

CREATE TEMP TABLE number_of_order_canceled AS 
	SELECT DATE_PART('YEAR', order_purchase_timestamp) AS years,
           COUNT(order_id) AS number_of_order_canceled
	FROM orders
	WHERE order_status = 'canceled'
	GROUP BY 1;

CREATE TEMP TABLE revenue_by_category_products_per_year AS
	SELECT years,
           product_category_name,
	       revenue
	FROM (
		SELECT DATE_PART('YEAR', t1.order_purchase_timestamp) AS years,
	           ROUND(SUM(t2.price + t2.freight_value)) AS revenue,
	           t3.product_category_name,
	           RANK() OVER (PARTITION BY DATE_PART('YEAR', t1.order_purchase_timestamp)
					        ORDER BY SUM(t2.price + t2.freight_value) DESC) AS rank_revenue
		FROM orders AS t1
		LEFT JOIN order_items AS t2 ON t2.order_id = t1.order_id
		LEFT JOIN products AS t3 ON t3.product_id = t2.product_id
		WHERE order_status NOT IN ('unavailable','invoiced','canceled')
	  	  	  AND product_category_name IS NOT NULL
		GROUP BY 1,3
		) AS rank_revenue_by_category_products_per_year
	WHERE rank_revenue = 1;

CREATE TEMP TABLE number_of_canceled_by_category_products_per_year AS
	SELECT years,
           product_category_name,
	       number_of_canceled
	FROM (
		SELECT DATE_PART('YEAR', t1.order_purchase_timestamp) AS years,
	           t3.product_category_name,
	           COUNT(t1.order_status) AS number_of_canceled,
	           RANK() OVER (PARTITION BY DATE_PART('YEAR', t1.order_purchase_timestamp)
					        ORDER BY COUNT(t1.order_status) DESC) AS rank_canceled
		FROM orders AS t1
		LEFT JOIN order_items AS t2 ON t2.order_id = t1.order_id
		LEFT JOIN products AS t3 ON t3.product_id = t2.product_id
		WHERE order_status = 'canceled'
	          AND product_category_name IS NOT NULL
		GROUP BY 1,2
		) AS rank_number_of_canceled_by_category_products_per_year
	WHERE rank_canceled = 1;

SELECT t1.years,
	   t1.revenue AS total_revenue,
       t2.number_of_order_canceled,
	   t3.product_category_name AS product_category_with_highest_total_revenue,
	   t3.revenue AS total_revenue_per_product_category,
	   t4.product_category_name AS product_category_with_highest_canceled,
	   t4.number_of_canceled AS number_of_canceled_per_product_category
FROM total_revenue AS t1
JOIN number_of_order_canceled AS t2 ON t2.years = t1.years
JOIN revenue_by_category_products_per_year AS t3 ON t3.years = t1.years
JOIN number_of_canceled_by_category_products_per_year AS t4 ON t4.years = t1.years;

/* E-commerce businesses generally provide an open-payment-based payment system that allows customers to
choose from various types of payment available. */

--Annual Payment Type Usage Analysis--
SELECT t1.payment_type,
       COUNT(t2.order_id)
FROM payments AS t1
RIGHT JOIN orders AS t2 ON t2.order_id = t1.order_id
WHERE order_status NOT IN ('unavailable','invoiced','canceled')
      AND payment_type IS NOT NULL
GROUP BY 1
ORDER BY 2 DESC;

WITH growth_of_payment AS (
	SELECT payment_type,
           SUM(CASE WHEN years = 2016 THEN number_of_order ELSE 0 END) AS year_2016,
	       SUM(CASE WHEN years = 2017 THEN number_of_order ELSE 0 END) AS year_2017,
	       SUM(CASE WHEN years = 2018 THEN number_of_order ELSE 0 END) AS year_2018
	FROM (
		SELECT DATE_PART('YEAR', t1.order_purchase_timestamp) AS years,
               t2.payment_type,
	           COUNT(t1.order_id) AS number_of_order
		FROM orders AS t1
		LEFT JOIN payments AS t2 ON t2.order_id = t1.order_id
		WHERE order_status NOT IN ('unavailable','invoiced','canceled')
      	      AND payment_type IS NOT NULL
		GROUP BY 1,2
		) AS count_payment
	GROUP BY 1
)
SELECT *,
	   ROUND(((year_2018 - year_2017) / year_2017), 4) * 100 AS growth_rate_2017_2018
FROM growth_of_payment
ORDER BY 5 DESC;