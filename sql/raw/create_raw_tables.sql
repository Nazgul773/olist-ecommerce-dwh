USE OlistDWH;
GO

-- olist_customers_dataset.csv
CREATE TABLE raw.[customers] (
    [customer_id] NVARCHAR(255),
    [customer_unique_id] NVARCHAR(255),
    [customer_zip_code_prefix] NVARCHAR(255),
    [customer_city] NVARCHAR(255),
    [customer_state] NVARCHAR(255)
);
GO

-- olist_geolocation_dataset.csv
CREATE TABLE raw.[geolocation] (
    [geolocation_zip_code_prefix] NVARCHAR(255),
    [geolocation_lat] NVARCHAR(255),
    [geolocation_lng] NVARCHAR(255),
    [geolocation_city] NVARCHAR(255),
    [geolocation_state] NVARCHAR(255)
);
GO

-- olist_order_items_dataset.csv
CREATE TABLE raw.[order_items] (
    [order_id] NVARCHAR(255),
    [order_item_id] NVARCHAR(255),
    [product_id] NVARCHAR(255),
    [seller_id] NVARCHAR(255),
    [shipping_limit_date] NVARCHAR(255),
    [price] NVARCHAR(255),
    [freight_value] NVARCHAR(255)
);
GO

-- olist_order_payments_dataset.csv
CREATE TABLE raw.[order_payments] (
    [order_id] NVARCHAR(255),
    [payment_sequential] NVARCHAR(255),
    [payment_type] NVARCHAR(255),
    [payment_installments] NVARCHAR(255),
    [payment_value] NVARCHAR(255)
);
GO

-- olist_order_reviews_dataset.csv
CREATE TABLE raw.[order_reviews] (
    [review_id] NVARCHAR(255),
    [order_id] NVARCHAR(255),
    [review_score] NVARCHAR(255),
    [review_comment_title] NVARCHAR(255),
    [review_comment_message] NVARCHAR(255),
    [review_creation_date] NVARCHAR(255),
    [review_answer_timestamp] NVARCHAR(255)
);
GO

-- olist_orders_dataset.csv
CREATE TABLE raw.[orders] (
    [order_id] NVARCHAR(255),
    [customer_id] NVARCHAR(255),
    [order_status] NVARCHAR(255),
    [order_purchase_timestamp] NVARCHAR(255),
    [order_approved_at] NVARCHAR(255),
    [order_delivered_carrier_date] NVARCHAR(255),
    [order_delivered_customer_date] NVARCHAR(255),
    [order_estimated_delivery_date] NVARCHAR(255)
);
GO

-- olist_products_dataset.csv
CREATE TABLE raw.[products] (
    [product_id] NVARCHAR(255),
    [product_category_name] NVARCHAR(255),
    [product_name_lenght] NVARCHAR(255),
    [product_description_lenght] NVARCHAR(255),
    [product_photos_qty] NVARCHAR(255),
    [product_weight_g] NVARCHAR(255),
    [product_length_cm] NVARCHAR(255),
    [product_height_cm] NVARCHAR(255),
    [product_width_cm] NVARCHAR(255)
);
GO

-- olist_sellers_dataset.csv
CREATE TABLE raw.[sellers] (
    [seller_id] NVARCHAR(255),
    [seller_zip_code_prefix] NVARCHAR(255),
    [seller_city] NVARCHAR(255),
    [seller_state] NVARCHAR(255)
);
GO

-- product_category_name_translation.csv
CREATE TABLE raw.[product_category_name_translation] (
    [product_category_name] NVARCHAR(255),
    [product_category_name_english] NVARCHAR(255)
);
GO

