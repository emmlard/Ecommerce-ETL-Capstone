-- create schema
CREATE SCHEMA IF NOT EXISTS raw;

-- create order_payments_dataset table
CREATE TABLE IF NOT EXISTS raw.order_payments_dataset (
	order_id UUID,
	payment_sequential integer,
	payment_type varchar,
	payment_installments integer,
	payment_value float
);
COPY raw.order_payments_dataset (order_id, payment_sequential, payment_type, payment_installments, payment_value)
FROM 'data-ingestion-postgresql/data/order_payments_dataset.csv' DELIMITER ',' CSV HEADER;

-- create order_items_dataset table
CREATE TABLE IF NOT EXISTS raw.order_items_dataset (
	order_id UUID,
	order_item_id integer,
	product_id UUID,
	seller_id UUID,
	shipping_limit_date timestamp,
	price float,
	freight_value float
);
COPY raw.order_items_dataset (order_id, order_item_id, product_id, seller_id, shipping_limit_date, price, freight_value)
FROM 'data-ingestion-postgresql/data/order_items_dataset.csv' DELIMITER ',' CSV HEADER;

-- create product_category_name_translation table
CREATE TABLE IF NOT EXISTS raw.product_category_name_translation (
	product_category_name varchar,
	product_category_name_english varchar
);
COPY raw.product_category_name_translation (product_category_name, product_category_name_english)
FROM 'data-ingestion-postgresql/data/product_category_name_translation.csv' DELIMITER ',' CSV HEADER;

-- create sellers_dataset table
CREATE TABLE IF NOT EXISTS raw.sellers_dataset (
	seller_id UUID,
	seller_zip_code_prefix varchar,
	seller_city varchar,
	seller_state varchar
);
COPY raw.sellers_dataset (seller_id, seller_zip_code_prefix, seller_city, seller_state)
FROM 'data-ingestion-postgresql/data/sellers_dataset.csv' DELIMITER ',' CSV HEADER;

-- create order_reviews_dataset table
CREATE TABLE IF NOT EXISTS raw.order_reviews_dataset (
	review_id UUID,
	order_id UUID,
	review_score integer,
	review_comment_title varchar,
	review_comment_message text,
	review_creation_date timestamp,
	review_answer_timestamp timestamp
);
COPY raw.order_reviews_dataset (review_id, order_id, review_score, review_comment_title, review_comment_message, review_creation_date, review_answer_timestamp)
FROM 'data-ingestion-postgresql/data/order_reviews_dataset.csv' DELIMITER ',' CSV HEADER;

-- create orders_dataset table
CREATE TABLE IF NOT EXISTS raw.orders_dataset (
	order_id UUID,
	customer_id UUID,
	order_status varchar,
	order_purchase_timestamp timestamp,
	order_approved_at timestamp,
	order_delivered_carrier_date timestamp,
	order_delivered_customer_date timestamp,
	order_estimated_delivery_date timestamp
);
COPY raw.orders_dataset (order_id, customer_id, order_status, order_purchase_timestamp, order_approved_at, order_delivered_carrier_date, order_delivered_customer_date, order_estimated_delivery_date)
FROM 'data-ingestion-postgresql/data/orders_dataset.csv' DELIMITER ',' CSV HEADER;

-- create products_dataset table
CREATE TABLE IF NOT EXISTS raw.products_dataset (
	product_id UUID,
	product_category_name varchar,
	product_name_lenght int,
	product_description_lenght int,
	product_photos_qty int,
	product_weight_g int,
	product_length_cm int,
	product_height_cm int,
	product_width_cm int
);
COPY raw.products_dataset (product_id, product_category_name, product_name_lenght, product_description_lenght, product_photos_qty, product_weight_g, product_length_cm, product_height_cm, product_width_cm)
FROM 'data-ingestion-postgresql/data/products_dataset.csv' DELIMITER ',' CSV HEADER;

-- create customers_dataset table
CREATE TABLE IF NOT EXISTS raw.customers_dataset (
	customer_id UUID,
	customer_unique_id UUID,
	customer_zip_code_prefix varchar,
	customer_city varchar,
	customer_state varchar
);
COPY raw.customers_dataset (customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state)
FROM 'data-ingestion-postgresql/data/customers_dataset.csv' DELIMITER ',' CSV HEADER;

-- create geolocation_dataset table
CREATE TABLE IF NOT EXISTS raw.geolocation_dataset (
	geolocation_zip_code_prefix varchar,
	geolocation_lat float,
	geolocation_lng float,
	geolocation_city varchar,
	geolocation_state varchar
);
COPY raw.geolocation_dataset (geolocation_zip_code_prefix, geolocation_lat, geolocation_lng, geolocation_city, geolocation_state)
FROM 'data-ingestion-postgresql/data/geolocation_dataset.csv' DELIMITER ',' CSV HEADER;
