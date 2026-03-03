CREATE SCHEMA warehouse

CREATE TABLE warehouse.dim_customer (
    customer_key SERIAL PRIMARY KEY,
    customer_id TEXT UNIQUE,
    customer_unique_id TEXT,
    customer_zip_code TEXT,
    customer_city TEXT,
    customer_state TEXT
);

INSERT INTO warehouse.dim_customer
(customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state)
SELECT DISTINCT
    customer_id,
    customer_unique_id,
    zip_code,
    city,
    state
FROM public.customer;

CREATE TABLE warehouse.dim_product (
    product_key SERIAL PRIMARY KEY,
    product_id TEXT UNIQUE,
    product_category_name TEXT
);

INSERT INTO warehouse.dim_product
(product_id, product_category_name)
SELECT DISTINCT
    product_id,
    product_category
FROM public.product;

CREATE TABLE warehouse.dim_time (
    time_key SERIAL PRIMARY KEY,
    order_date DATE UNIQUE,
    year INT,
    month INT,
    day INT,
    quarter INT
);

INSERT INTO warehouse.dim_time (order_date, year, month, day, quarter)
SELECT DISTINCT
    DATE(order_purchase_timestamp),
    EXTRACT(YEAR FROM order_purchase_timestamp),
    EXTRACT(MONTH FROM order_purchase_timestamp),
    EXTRACT(DAY FROM order_purchase_timestamp),
    EXTRACT(QUARTER FROM order_purchase_timestamp)
FROM public.orders
ORDER BY 1;

CREATE TABLE warehouse.fact_sales (
    sales_key SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_key INT,
    product_key INT,
    time_key INT,
    order_item_id INT,
    price NUMERIC,
    freight_value NUMERIC
);

INSERT INTO warehouse.fact_sales
(order_id, customer_key, product_key, time_key, order_item_id, price, freight_value)
SELECT
    oi.order_id,
    dc.customer_key,
    dp.product_key,
    dt.time_key,
    oi.order_item_id,
    oi.price,
    oi.freight_value
FROM public.order_items oi
JOIN public.orders o ON oi.order_id = o.order_id
JOIN warehouse.dim_customer dc ON o.customer_id = dc.customer_id
JOIN warehouse.dim_product dp ON oi.product_id = dp.product_id
JOIN warehouse.dim_time dt ON DATE(o.order_purchase_timestamp) = dt.order_date;

ALTER TABLE warehouse.fact_sales
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_key)
REFERENCES warehouse.dim_customer(customer_key);

ALTER TABLE warehouse.fact_sales
ADD CONSTRAINT fk_product
FOREIGN KEY (product_key)
REFERENCES warehouse.dim_product(product_key);

ALTER TABLE warehouse.fact_sales
ADD CONSTRAINT fk_time
FOREIGN KEY (time_key)
REFERENCES warehouse.dim_time(time_key);

CREATE TABLE warehouse.fact_reviews (
    review_key SERIAL PRIMARY KEY,
    customer_key INT,
    product_key INT,
    review_score INT,
    review_message TEXT
);

INSERT INTO warehouse.fact_reviews (customer_key, product_key, review_score, review_message)
SELECT
    dc.customer_key,
    dp.product_key,
    r.review_score,
    r.review_message
FROM public.review r
JOIN public.orders o
    ON r.order_id = o.order_id
JOIN warehouse.dim_customer dc
    ON o.customer_id = dc.customer_id
JOIN public.order_items oi
    ON o.order_id = oi.order_id
JOIN warehouse.dim_product dp
    ON oi.product_id = dp.product_id; 

ALTER TABLE warehouse.fact_reviews
ADD CONSTRAINT fk_customer
FOREIGN KEY (customer_key)
REFERENCES warehouse.dim_customer(customer_key);

ALTER TABLE warehouse.fact_reviews
ADD CONSTRAINT fk_product
FOREIGN KEY (product_key)
REFERENCES warehouse.dim_product(product_key);

CREATE TABLE warehouse.fact_payments (
    payment_key SERIAL PRIMARY KEY,
    order_id TEXT,
    customer_key INT,
    payment_type TEXT,
    payment_value NUMERIC
);

INSERT INTO warehouse.fact_payments (order_id, customer_key, payment_type, payment_value)
SELECT
    p.order_id,
    dc.customer_key,
    p.payment_type,
    p.payment_value
FROM public.payment p
JOIN public.orders o ON p.order_id = o.order_id
JOIN warehouse.dim_customer dc ON o.customer_id = dc.customer_id;

ALTER TABLE warehouse.fact_payments
ADD CONSTRAINT fk_customer  
FOREIGN KEY (customer_key)
REFERENCES warehouse.dim_customer(customer_key);

ALTER TABLE warehouse.fact_payments
ADD CONSTRAINT fk_order
FOREIGN KEY (order_id)
REFERENCES public.orders(order_id);

