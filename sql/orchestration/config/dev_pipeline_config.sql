USE OlistDWH;
GO

-- Idempotent pipeline configuration seeding - DEV environment
-- Run this script to populate pipeline_config table with default values
--
-- SETUP (required before running):
--   Set @DatasetRoot below to the folder containing the Olist CSV files.
--   Trailing backslash required. All RAW file_path values are derived from it.

DECLARE @DatasetRoot NVARCHAR(500) = 'D:\Code\VCS Projects\olist-ecommerce-dwh\data\';

-- RAW Layer
IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'customers' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence, needs_preprocessing)
    VALUES ('RAW', 'customers', 'raw.sp_load_customers', @DatasetRoot + 'olist_customers_dataset_pipe.csv', 'olist_customers_dataset_pipe.csv', 1, 1);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'orders' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence)
    VALUES ('RAW', 'orders', 'raw.sp_load_orders', @DatasetRoot + 'olist_orders_dataset.csv', 'olist_orders_dataset.csv', 2);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_items' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence)
    VALUES ('RAW', 'order_items', 'raw.sp_load_order_items', @DatasetRoot + 'olist_order_items_dataset.csv', 'olist_order_items_dataset.csv', 3);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'geolocation' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence, needs_preprocessing)
    VALUES ('RAW', 'geolocation', 'raw.sp_load_geolocation', @DatasetRoot + 'olist_geolocation_dataset_pipe.csv', 'olist_geolocation_dataset_pipe.csv', 4, 1);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_payments' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence)
    VALUES ('RAW', 'order_payments', 'raw.sp_load_order_payments', @DatasetRoot + 'olist_order_payments_dataset.csv', 'olist_order_payments_dataset.csv', 5);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_reviews' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence, needs_preprocessing)
    VALUES ('RAW', 'order_reviews', 'raw.sp_load_order_reviews', @DatasetRoot + 'olist_order_reviews_dataset_pipe.csv', 'olist_order_reviews_dataset_pipe.csv', 6, 1);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'products' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence)
    VALUES ('RAW', 'products', 'raw.sp_load_products', @DatasetRoot + 'olist_products_dataset.csv', 'olist_products_dataset.csv', 7);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'sellers' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence, needs_preprocessing)
    VALUES ('RAW', 'sellers', 'raw.sp_load_sellers', @DatasetRoot + 'olist_sellers_dataset_pipe.csv', 'olist_sellers_dataset_pipe.csv', 8, 1);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'product_category_translation' AND layer = 'RAW')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, file_path, file_name, load_sequence)
    VALUES ('RAW', 'product_category_translation', 'raw.sp_load_product_category_name_translation', @DatasetRoot + 'product_category_name_translation.csv', 'product_category_name_translation.csv', 9);

-- CLEANSED Layer
-- source_pipeline_id resolved via subquery — avoids hardcoding IDENTITY values.
IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'customers' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'customers', 'cleansed.sp_load_customers', 1,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'customers' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'orders' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'orders', 'cleansed.sp_load_orders', 2,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'orders' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_items' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'order_items', 'cleansed.sp_load_order_items', 3,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'order_items' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'geolocation' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'geolocation', 'cleansed.sp_load_geolocation', 4,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'geolocation' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_payments' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'order_payments', 'cleansed.sp_load_order_payments', 5,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'order_payments' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'order_reviews' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'order_reviews', 'cleansed.sp_load_order_reviews', 6,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'order_reviews' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'products' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'products', 'cleansed.sp_load_products', 7,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'products' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'sellers' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'sellers', 'cleansed.sp_load_sellers', 8,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'sellers' AND layer = 'RAW';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'product_category_translation' AND layer = 'CLEANSED')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence, source_pipeline_id)
    SELECT 'CLEANSED', 'product_category_translation', 'cleansed.sp_load_product_category_name_translation', 9,
           pipeline_id FROM orchestration.pipeline_config WHERE table_name = 'product_category_translation' AND layer = 'RAW';

-- MART Layer
IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'fact_sales' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'fact_sales', 'mart.sp_load_fact_sales', 1);
GO
