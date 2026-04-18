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

    -- Inherit the RAW batch_id so the same ID flows through RAW and CLEANSED,
    -- enabling layer-to-layer tracing via batch_id. Cross-layer (mart) tracing
    -- uses job_run_id, which flows through all three layers.
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
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE,
        --      'ranked'     — applies ROW_NUMBER() for deduplication on product_category_name.
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same product_category_name, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same product_category_name, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        ;WITH normalized AS (
            SELECT
                product_category_name,
                product_category_name_english,
                REPLACE(TRIM(product_category_name),         '"', '') AS clean_pt,
                REPLACE(TRIM(product_category_name_english), '"', '') AS clean_en
            FROM raw.product_category_name_translation
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(clean_pt, '|', clean_en)) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_pt
                    ORDER BY clean_en
                ) AS rn
            FROM hashed
        )
        SELECT
            product_category_name, product_category_name_english,
            clean_pt, clean_en,
            row_hash, rn
        INTO #normalized_translations
        FROM ranked;

        -- 2. DQ checks: completeness, uniqueness.
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

            -- Duplicates Type A: same product_category_name, identical content — load artefact
            UNION ALL
            SELECT 'product_category_name', 'Duplicate product_category_name: identical content, deduplicated silently'
            FROM #normalized_translations n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_translations canon
                  WHERE canon.clean_pt   = n.clean_pt
                    AND canon.rn         = 1
                    AND canon.row_hash   = n.row_hash
              )

            -- Duplicates Type B: same product_category_name, conflicting content — data quality conflict
            UNION ALL
            SELECT 'product_category_name', 'Duplicate product_category_name: conflicting content — investigate before reload'
            FROM #normalized_translations n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_translations canon
                  WHERE canon.clean_pt   = n.clean_pt
                    AND canon.rn         = 1
                    AND canon.row_hash   = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'product_category_name_translation', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same product_category_name
        -- cannot be resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'product_category_name_translation'
              AND column_name = 'product_category_name'
              AND issue LIKE 'Duplicate product_category_name: conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate product_category_name values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per product_category_name) enter the MERGE as source.
        BEGIN TRANSACTION;
        MERGE cleansed.product_category_name_translation AS tgt
        USING (
            SELECT *
            FROM #normalized_translations
            WHERE rn = 1
              AND clean_pt IS NOT NULL AND clean_pt != ''
              AND clean_en IS NOT NULL AND clean_en != ''
        ) AS src
        ON tgt.product_category_name = src.clean_pt
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                product_category_name_english = src.clean_en,
                row_hash                      = src.row_hash,
                is_deleted                    = 0,
                deleted_at                    = NULL,
                updated_at                    = SYSUTCDATETIME()
        -- Soft delete: is_deleted = 1 + deleted_at, not a hard delete.
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
