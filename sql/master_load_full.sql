-- ============================================================
-- master-load - full
-- Full pipeline: raw -> cleansed
-- ============================================================
USE OlistDWH;
GO

DECLARE @base      NVARCHAR(500) = 'D:\Code\Datasets\olist_data\';
DECLARE @file_path NVARCHAR(500);

DECLARE @batch_id_customers UNIQUEIDENTIFIER;

-- ============================================================
-- 1. Raw load
-- ============================================================

SET @file_path = @base + 'olist_customers_dataset.csv';
EXEC raw.sp_load_customers
    @file_path = @file_path,
    @file_name = 'olist_customers_dataset.csv',
    @batch_id  = @batch_id_customers OUTPUT;

PRINT '=> raw load complete';

-- ============================================================
-- 2. Cleansed load
-- ============================================================

EXEC cleansed.sp_load_customers @batch_id = @batch_id_customers;

PRINT '=> cleansed load complete';
