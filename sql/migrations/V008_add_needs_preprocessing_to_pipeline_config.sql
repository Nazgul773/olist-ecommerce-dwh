USE OlistDWH;
GO

-- Migration: V008
-- Description: Add needs_preprocessing column to orchestration.pipeline_config.
--              Replaces the implicit _pipe.csv naming convention with an explicit flag.
--              Backfills existing rows: sets 1 for RAW entries whose file_name ends with _pipe.csv.
-- Applied: manually in SSMS

ALTER TABLE orchestration.pipeline_config
    ADD needs_preprocessing BIT NOT NULL DEFAULT 0;
GO

UPDATE orchestration.pipeline_config
SET    needs_preprocessing = 1
WHERE  layer     = 'RAW'
  AND  file_name LIKE '%_pipe.csv';
GO
