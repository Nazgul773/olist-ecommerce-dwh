USE OlistDWH;
GO

-- Migration: V009
-- Description: Add MART layer pipeline_config entries for all dimension and fact SPs.
--              The existing fact_sales entry (load_sequence=1) is updated to
--              load_sequence=6 so dimensions run first in the correct order.
--              Run this AFTER deploying all mart schema DDL and mart SPs.

-- Correct the existing fact_sales entry: sequence 1 -> 6 (dims run first)
UPDATE orchestration.pipeline_config
SET load_sequence = 6
WHERE table_name = 'fact_sales' AND layer = 'MART';

-- Dimensions (load_sequence 1–5): no file_path, no source_pipeline_id
IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_date' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_date', 'mart.sp_load_dim_date', 1);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_customer' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_customer', 'mart.sp_load_dim_customer', 2);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_seller' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_seller', 'mart.sp_load_dim_seller', 3);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_product' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_product', 'mart.sp_load_dim_product', 4);

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'dim_payment_type' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'dim_payment_type', 'mart.sp_load_dim_payment_type', 5);

-- Facts (load_sequence 6–7): depend on all dims being populated
-- fact_sales entry already exists (created in initial seeding) — update sp_name
-- in case it was not previously set correctly, then skip re-insert.
UPDATE orchestration.pipeline_config
SET sp_name = 'mart.sp_load_fact_sales'
WHERE table_name = 'fact_sales' AND layer = 'MART';

IF NOT EXISTS (SELECT 1 FROM orchestration.pipeline_config WHERE table_name = 'fact_payments' AND layer = 'MART')
    INSERT INTO orchestration.pipeline_config (layer, table_name, sp_name, load_sequence)
    VALUES ('MART', 'fact_payments', 'mart.sp_load_fact_payments', 7);
GO
