USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_products
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
            'CLEANSED',     'cleansed.sp_load_products', 'cleansed.products',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE.        --      'ranked'     — applies ROW_NUMBER() for deduplication on product_id.
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same product_id, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same product_id, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        ;WITH normalized AS (
            SELECT
                product_id,
                product_category_name,
                product_name_lenght,
                product_description_lenght,
                product_photos_qty,
                product_weight_g,
                product_length_cm,
                product_height_cm,
                product_width_cm,
                REPLACE(TRIM(product_id),            '"', '') AS clean_product_id,
                REPLACE(TRIM(product_category_name), '"', '') AS clean_category_name,
                TRY_CAST(TRIM(product_name_lenght)        AS INT) AS parsed_name_lenght,
                TRY_CAST(TRIM(product_description_lenght) AS INT) AS parsed_description_lenght,
                TRY_CAST(TRIM(product_photos_qty)         AS INT) AS parsed_photos_qty,
                CASE WHEN TRY_CAST(TRIM(product_weight_g)  AS INT) > 0 THEN TRY_CAST(TRIM(product_weight_g)  AS INT) ELSE NULL END AS parsed_weight_g,
                CASE WHEN TRY_CAST(TRIM(product_length_cm) AS INT) > 0 THEN TRY_CAST(TRIM(product_length_cm) AS INT) ELSE NULL END AS parsed_length_cm,
                CASE WHEN TRY_CAST(TRIM(product_height_cm) AS INT) > 0 THEN TRY_CAST(TRIM(product_height_cm) AS INT) ELSE NULL END AS parsed_height_cm,
                CASE WHEN TRY_CAST(TRIM(product_width_cm)  AS INT) > 0 THEN TRY_CAST(TRIM(product_width_cm)  AS INT) ELSE NULL END AS parsed_width_cm
            FROM raw.products
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_product_id,                                        '|',
                    ISNULL(NULLIF(clean_category_name, ''),             ''), '|',
                    ISNULL(CAST(parsed_name_lenght        AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_description_lenght AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_photos_qty         AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_weight_g           AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_length_cm          AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_height_cm          AS NVARCHAR), ''), '|',
                    ISNULL(CAST(parsed_width_cm           AS NVARCHAR), '')
                )) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_product_id
                    ORDER BY ISNULL(clean_category_name, '')
                ) AS rn
            FROM hashed
        )
        SELECT
            product_id, product_category_name,
            product_name_lenght, product_description_lenght, product_photos_qty,
            product_weight_g, product_length_cm, product_height_cm, product_width_cm,
            clean_product_id, clean_category_name,
            parsed_name_lenght, parsed_description_lenght, parsed_photos_qty,
            parsed_weight_g, parsed_length_cm, parsed_height_cm, parsed_width_cm,
            row_hash, rn
        INTO #normalized_products
        FROM ranked;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: product_id must not be NULL
            SELECT 'product_id' AS column_name, 'NULL value' AS issue FROM #normalized_products WHERE clean_product_id IS NULL

            -- Completeness: empty string check
            UNION ALL
            SELECT 'product_id', 'Empty string after cleansing' FROM #normalized_products WHERE clean_product_id = ''

            -- Validity: product_id length + hex format
            UNION ALL
            SELECT 'product_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_products WHERE clean_product_id != '' AND (LEN(clean_product_id) != 32 OR clean_product_id LIKE '%[^0-9a-f]%')

            -- Validity: numeric parse failures for dimension columns
            UNION ALL
            SELECT 'product_name_lenght',        'Invalid numeric format' FROM #normalized_products WHERE product_name_lenght        IS NOT NULL AND parsed_name_lenght        IS NULL
            UNION ALL
            SELECT 'product_description_lenght', 'Invalid numeric format' FROM #normalized_products WHERE product_description_lenght IS NOT NULL AND parsed_description_lenght IS NULL
            UNION ALL
            SELECT 'product_photos_qty',         'Invalid numeric format' FROM #normalized_products WHERE product_photos_qty         IS NOT NULL AND parsed_photos_qty         IS NULL
            UNION ALL
            SELECT 'product_weight_g',           'Invalid numeric format' FROM #normalized_products WHERE product_weight_g  IS NOT NULL AND TRY_CAST(TRIM(product_weight_g)  AS INT) IS NULL
            UNION ALL
            SELECT 'product_length_cm',          'Invalid numeric format' FROM #normalized_products WHERE product_length_cm IS NOT NULL AND TRY_CAST(TRIM(product_length_cm) AS INT) IS NULL
            UNION ALL
            SELECT 'product_height_cm',          'Invalid numeric format' FROM #normalized_products WHERE product_height_cm IS NOT NULL AND TRY_CAST(TRIM(product_height_cm) AS INT) IS NULL
            UNION ALL
            SELECT 'product_width_cm',           'Invalid numeric format' FROM #normalized_products WHERE product_width_cm  IS NOT NULL AND TRY_CAST(TRIM(product_width_cm)  AS INT) IS NULL

            -- Validity: zero or negative dimension values — stored as NULL in cleansed
            UNION ALL
            SELECT 'product_weight_g',  'Invalid range: must be > 0; stored as NULL' FROM #normalized_products WHERE TRY_CAST(TRIM(product_weight_g)  AS INT) IS NOT NULL AND TRY_CAST(TRIM(product_weight_g)  AS INT) <= 0
            UNION ALL
            SELECT 'product_length_cm', 'Invalid range: must be > 0; stored as NULL' FROM #normalized_products WHERE TRY_CAST(TRIM(product_length_cm) AS INT) IS NOT NULL AND TRY_CAST(TRIM(product_length_cm) AS INT) <= 0
            UNION ALL
            SELECT 'product_height_cm', 'Invalid range: must be > 0; stored as NULL' FROM #normalized_products WHERE TRY_CAST(TRIM(product_height_cm) AS INT) IS NOT NULL AND TRY_CAST(TRIM(product_height_cm) AS INT) <= 0
            UNION ALL
            SELECT 'product_width_cm',  'Invalid range: must be > 0; stored as NULL' FROM #normalized_products WHERE TRY_CAST(TRIM(product_width_cm)  AS INT) IS NOT NULL AND TRY_CAST(TRIM(product_width_cm)  AS INT) <= 0

            -- Duplicates Type A: same product_id, identical content (hash match) — load artefact
            UNION ALL
            SELECT 'product_id', 'Duplicate product_id: identical content, deduplicated silently'
            FROM #normalized_products n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_products canon
                  WHERE canon.clean_product_id = n.clean_product_id
                    AND canon.rn               = 1
                    AND canon.row_hash         = n.row_hash
              )

            -- Duplicates Type B: same product_id, conflicting content (hash mismatch) — data quality conflict
            UNION ALL
            SELECT 'product_id', 'Duplicate product_id: conflicting content — investigate before reload'
            FROM #normalized_products n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_products canon
                  WHERE canon.clean_product_id = n.clean_product_id
                    AND canon.rn               = 1
                    AND canon.row_hash         = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'products', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same product_id cannot be
        -- resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'products'
              AND column_name = 'product_id'
              AND issue LIKE 'Duplicate product_id: conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate product_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per product_id) enter the MERGE as source.
        --    NULLIF applied to clean_category_name: empty string stored as NULL in cleansed.
        BEGIN TRANSACTION;
        MERGE cleansed.products AS tgt
        USING (
            SELECT *
            FROM #normalized_products
            WHERE rn = 1
              AND clean_product_id IS NOT NULL AND clean_product_id != '' AND clean_product_id NOT LIKE '%[^0-9a-f]%' AND LEN(clean_product_id) = 32
        ) AS src
        ON tgt.product_id = src.clean_product_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                product_category_name      = NULLIF(src.clean_category_name, ''),
                product_name_lenght        = src.parsed_name_lenght,
                product_description_lenght = src.parsed_description_lenght,
                product_photos_qty         = src.parsed_photos_qty,
                product_weight_g           = src.parsed_weight_g,
                product_length_cm          = src.parsed_length_cm,
                product_height_cm          = src.parsed_height_cm,
                product_width_cm           = src.parsed_width_cm,
                row_hash                   = src.row_hash,
                is_deleted                 = 0,
                deleted_at                 = NULL,
                updated_at                 = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                product_id,                 product_category_name,
                product_name_lenght,        product_description_lenght,
                product_photos_qty,         product_weight_g,
                product_length_cm,          product_height_cm,
                product_width_cm,           row_hash,              updated_at
            )
            VALUES (
                src.clean_product_id,       NULLIF(src.clean_category_name, ''),
                src.parsed_name_lenght,     src.parsed_description_lenght,
                src.parsed_photos_qty,      src.parsed_weight_g,
                src.parsed_length_cm,       src.parsed_height_cm,
                src.parsed_width_cm,        src.row_hash,          SYSUTCDATETIME()
            )
        -- Soft delete: is_deleted = 1 + deleted_at, not a hard delete.
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_products';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_products';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_products',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
