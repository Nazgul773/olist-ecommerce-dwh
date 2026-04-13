USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_product_category_name_translation
    @batch_id    UNIQUEIDENTIFIER OUTPUT,
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @merge_rowcount INT           = 0;
    DECLARE @start_time     DATETIME2(3)  = SYSUTCDATETIME();
    DECLARE @duration_ms    INT;
    DECLARE @error_msg      NVARCHAR(MAX);

    -- Inherit the RAW batch_id so the same ID flows through all layers
    -- (raw → cleansed → mart), enabling end-to-end tracing.
    SELECT @batch_id = src.last_batch_id
    FROM orchestration.pipeline_config cleansed_cfg
    JOIN orchestration.pipeline_config src
        ON src.pipeline_id      = cleansed_cfg.source_pipeline_id
    WHERE cleansed_cfg.pipeline_id = @pipeline_id
      AND src.last_run_status      = 'SUCCESS';

    BEGIN TRY
        INSERT INTO audit.load_log (
            batch_id,       job_run_id,  pipeline_id,
            layer,          sp_name,     table_name,
            rows_processed, status,      load_ts
        )
        VALUES (
            @batch_id,      @job_run_id, @pipeline_id,
            'CLEANSED',     'cleansed.sp_load_product_category_name_translation', 'cleansed.product_category_name_translation',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            row_id,
            product_category_name,
            product_category_name_english,
            REPLACE(TRIM(product_category_name),         '"', '') AS clean_pt,
            REPLACE(TRIM(product_category_name_english), '"', '') AS clean_en
        INTO #normalized_translations
        FROM raw.product_category_name_translation
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'product_category_name'         AS column_name, 'NULL value' AS issue FROM #normalized_translations WHERE clean_pt IS NULL
            UNION ALL
            SELECT 'product_category_name_english', 'NULL value'                         FROM #normalized_translations WHERE clean_en IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT 'product_category_name',         'Empty string after cleansing' FROM #normalized_translations WHERE clean_pt = ''
            UNION ALL
            SELECT 'product_category_name_english', 'Empty string after cleansing' FROM #normalized_translations WHERE clean_en = ''

            -- Uniqueness: one row per duplicate occurrence so outer GROUP BY counts total
            UNION ALL
            SELECT 'product_category_name', 'Duplicate product_category_name in batch'
            FROM (SELECT COUNT(*) OVER (PARTITION BY product_category_name) AS cnt FROM #normalized_translations) d
            WHERE cnt > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'product_category_name_translation', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'product_category_name_translation'
              AND column_name = 'product_category_name'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate product_category_name values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_pt,
                clean_en,
                HASHBYTES('SHA2_256', CONCAT(clean_pt, '|', clean_en)) AS row_hash
            FROM #normalized_translations
        )
        MERGE cleansed.product_category_name_translation AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_pt IS NOT NULL AND clean_pt != ''
              AND clean_en IS NOT NULL AND clean_en != ''
        ) AS src
        ON tgt.product_category_name = src.clean_pt
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                product_category_name_english = src.clean_en,
                row_hash                      = src.row_hash,
                is_deleted                    = 0,
                deleted_at                    = NULL,
                updated_at                    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                product_category_name, product_category_name_english,
                row_hash,              updated_at
            )
            VALUES (
                src.clean_pt, src.clean_en,
                src.row_hash, SYSUTCDATETIME()
            )
        WHEN NOT MATCHED BY SOURCE AND tgt.is_deleted = 0 THEN
            UPDATE SET
                is_deleted = 1,
                deleted_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME();

        SET @merge_rowcount = @@ROWCOUNT;
        SET @duration_ms    = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @merge_rowcount,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_product_category_name_translation';

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0
            ROLLBACK TRANSACTION;

        SET @error_msg   = ERROR_MESSAGE();
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET status                = 'FAILED',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_product_category_name_translation';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_product_category_name_translation',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
