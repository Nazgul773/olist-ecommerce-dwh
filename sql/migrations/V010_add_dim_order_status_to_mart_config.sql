USE OlistDWH;
GO

-- Migration V010: Add dim_order_status pipeline to MART layer config.
-- Description: dim_order_status is a static lookup seeded by mart.sp_load_dim_order_status.
--              Inserted at load_sequence = 6 (before fact_sales) — fact_sales depends on it
--              for order_status_key resolution. fact_sales and fact_payments are shifted to 7 and 8.
-- Applied: manually in SSMS

UPDATE orchestration.pipeline_config
SET load_sequence = 7
WHERE table_name = 'fact_sales' AND layer = 'MART';

UPDATE orchestration.pipeline_config
SET load_sequence = 8
WHERE table_name = 'fact_payments' AND layer = 'MART';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_order_status' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_order_status', 'mart.sp_load_dim_order_status', 6);
GO
