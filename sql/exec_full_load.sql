-- ============================================================
-- Executes the full load pipeline across all layers.
-- Raw:      Loads source CSV files via BULK INSERT (append-only, batch-tracked).
-- Cleansed: Applies incremental MERGE with row-hash-based change detection, DQ checks and error logging.
-- Mart:     Joins and aggregates cleansed data into Star-Schema fact and dimension tables. (not implemented yet)
-- ============================================================
USE OlistDWH;
GO

DECLARE @base NVARCHAR(500) = 'D:\Code\Datasets\olist_data\';
DECLARE @file_path NVARCHAR(500);

DECLARE @batch_id_customers                     UNIQUEIDENTIFIER;
DECLARE @batch_id_orders                        UNIQUEIDENTIFIER;
DECLARE @batch_id_order_items                   UNIQUEIDENTIFIER;
DECLARE @batch_id_geolocation                   UNIQUEIDENTIFIER;
DECLARE @batch_id_order_payments                UNIQUEIDENTIFIER;
DECLARE @batch_id_order_reviews                 UNIQUEIDENTIFIER;
DECLARE @batch_id_products                      UNIQUEIDENTIFIER;
DECLARE @batch_id_sellers                       UNIQUEIDENTIFIER;
DECLARE @batch_id_product_category_translation  UNIQUEIDENTIFIER;

-- ============================================================
-- 1. Raw load
-- ============================================================

PRINT '------------------raw load starting---------------------'
PRINT '--------------------------------------------------------'

SET @file_path = @base + 'olist_customers_dataset.csv';
EXEC raw.sp_load_customers
    @file_path = @file_path,
    @file_name = 'olist_customers_dataset.csv',
    @batch_id  = @batch_id_customers OUTPUT;

SET @file_path = @base + 'olist_orders_dataset.csv';
EXEC raw.sp_load_orders
    @file_path = @file_path,
    @file_name = 'olist_orders_dataset.csv',
    @batch_id  = @batch_id_orders OUTPUT;

SET @file_path = @base + 'olist_order_items_dataset.csv';
EXEC raw.sp_load_order_items
    @file_path = @file_path,
    @file_name = 'olist_order_items_dataset.csv',
    @batch_id  = @batch_id_order_items OUTPUT;

SET @file_path = @base + 'olist_geolocation_dataset.csv';
EXEC raw.sp_load_geolocation
    @file_path = @file_path,
    @file_name = 'olist_geolocation_dataset.csv',
    @batch_id  = @batch_id_geolocation OUTPUT;

SET @file_path = @base + 'olist_order_payments_dataset.csv';
EXEC raw.sp_load_order_payments
    @file_path = @file_path,
    @file_name = 'olist_order_payments_dataset.csv',
    @batch_id  = @batch_id_order_payments OUTPUT;

SET @file_path = @base + 'olist_order_reviews_dataset.csv';
EXEC raw.sp_load_order_reviews
    @file_path = @file_path,
    @file_name = 'olist_order_reviews_dataset.csv',
    @batch_id  = @batch_id_order_reviews OUTPUT;

SET @file_path = @base + 'olist_products_dataset.csv';
EXEC raw.sp_load_products
    @file_path = @file_path,
    @file_name = 'olist_products_dataset.csv',
    @batch_id  = @batch_id_products OUTPUT;

SET @file_path = @base + 'olist_sellers_dataset.csv';
EXEC raw.sp_load_sellers
    @file_path = @file_path,
    @file_name = 'olist_sellers_dataset.csv',
    @batch_id  = @batch_id_sellers OUTPUT;

SET @file_path = @base + 'product_category_name_translation.csv';
EXEC raw.sp_load_product_category_name_translation
    @file_path = @file_path,
    @file_name = 'product_category_name_translation.csv',
    @batch_id  = @batch_id_product_category_translation OUTPUT;


PRINT '------------------raw load complete---------------------'
PRINT '--------------------------------------------------------'

-- ============================================================
-- 2. Cleansed load
-- ============================================================

PRINT '----------------cleanse load starting-------------------'
PRINT '--------------------------------------------------------'

EXEC cleansed.sp_load_customers
    @batch_id = @batch_id_customers;

EXEC cleansed.sp_load_orders
    @batch_id = @batch_id_orders;

EXEC cleansed.sp_load_order_items
    @batch_id = @batch_id_order_items;

EXEC cleansed.sp_load_geolocation
    @batch_id = @batch_id_geolocation;

EXEC cleansed.sp_load_order_payments
    @batch_id = @batch_id_order_payments;

EXEC cleansed.sp_load_order_reviews
    @batch_id = @batch_id_order_reviews;

EXEC cleansed.sp_load_products
    @batch_id = @batch_id_products;

EXEC cleansed.sp_load_sellers
    @batch_id = @batch_id_sellers;

EXEC cleansed.sp_load_product_category_name_translation
    @batch_id = @batch_id_product_category_translation;

PRINT '----------------cleanse load complete-------------------'
PRINT '--------------------------------------------------------'
