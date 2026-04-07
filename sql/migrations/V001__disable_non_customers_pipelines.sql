USE OlistDWH;
GO

-- Migration: V001
-- Description: Disable all pipelines except 'customers' (RAW + CLEANSED)
--              during initial development phase. Remaining entities will be
--              activated incrementally as their stored procedures are completed.
-- Applied: manually in SSMS

UPDATE orchestration.pipeline_config
SET    is_active = 0
WHERE  table_name <> 'customers';
GO
