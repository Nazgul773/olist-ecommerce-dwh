USE OlistDWH;
GO

-- Migration: V002
-- Description: Activate pipelines for orders and order_items (RAW + CLEANSED)
--              . Remaining entities will be activated incrementally as their stored procedures are completed.
-- Applied: manually in SSMS

UPDATE orchestration.pipeline_config
SET    is_active = 1
WHERE  table_name IN ('orders', 'order_items');
GO
