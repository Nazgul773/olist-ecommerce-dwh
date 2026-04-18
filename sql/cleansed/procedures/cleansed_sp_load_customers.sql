USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_customers
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
            'CLEANSED',     'cleansed.sp_load_customers', 'cleansed.customers',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE.
        --      'ranked'     — applies ROW_NUMBER() for deduplication on customer_id.
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same customer_id, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same customer_id, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        ;WITH normalized AS (
            SELECT
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                REPLACE(TRIM(customer_id),              '"', '') AS clean_customer_id,
                REPLACE(TRIM(customer_unique_id),        '"', '') AS clean_customer_unique_id,
                REPLACE(TRIM(customer_zip_code_prefix),  '"', '') AS clean_customer_zip_code_prefix,
                dbo.fn_normalize_text(customer_city)              AS clean_customer_city,
                dbo.fn_normalize_text(customer_state)             AS clean_customer_state
            FROM raw.customers
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_customer_id,              '|',
                    clean_customer_unique_id,        '|',
                    clean_customer_zip_code_prefix,  '|',
                    clean_customer_city,             '|',
                    clean_customer_state
                )) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_customer_id
                    ORDER BY clean_customer_unique_id, clean_customer_zip_code_prefix
                ) AS rn
            FROM hashed
        )
        SELECT
            customer_id, customer_unique_id, customer_zip_code_prefix, customer_city, customer_state,
            clean_customer_id, clean_customer_unique_id, clean_customer_zip_code_prefix,
            clean_customer_city, clean_customer_state,
            row_hash, rn
        INTO #normalized_customers
        FROM ranked;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'customer_id'              AS column_name, 'NULL value' AS issue FROM #normalized_customers WHERE clean_customer_id IS NULL
            UNION ALL
            SELECT 'customer_unique_id',       'NULL value'                         FROM #normalized_customers WHERE clean_customer_unique_id IS NULL
            UNION ALL
            SELECT 'customer_zip_code_prefix', 'NULL value'                         FROM #normalized_customers WHERE clean_customer_zip_code_prefix IS NULL
            UNION ALL
            SELECT 'customer_city',            'NULL value'                         FROM #normalized_customers WHERE clean_customer_city IS NULL
            UNION ALL
            SELECT 'customer_state',           'NULL value'                         FROM #normalized_customers WHERE clean_customer_state IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT 'customer_id',              'Empty string after cleansing'        FROM #normalized_customers WHERE clean_customer_id = ''
            UNION ALL
            SELECT 'customer_unique_id',       'Empty string after cleansing'        FROM #normalized_customers WHERE clean_customer_unique_id = ''
            UNION ALL
            SELECT 'customer_zip_code_prefix', 'Empty string after cleansing'        FROM #normalized_customers WHERE clean_customer_zip_code_prefix = ''
            UNION ALL
            SELECT 'customer_city',            'Empty string after cleansing'        FROM #normalized_customers WHERE clean_customer_city = ''
            UNION ALL
            SELECT 'customer_state',           'Empty string after cleansing'        FROM #normalized_customers WHERE clean_customer_state = ''

            -- Validity: format and length checks
            UNION ALL
            SELECT 'customer_id',              'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_customers WHERE clean_customer_id != ''              AND (LEN(clean_customer_id) != 32 OR clean_customer_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'customer_unique_id',       'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_customers WHERE clean_customer_unique_id != ''       AND (LEN(clean_customer_unique_id) != 32 OR clean_customer_unique_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'customer_zip_code_prefix', 'Invalid length or format: expected 5 numeric digits'     FROM #normalized_customers WHERE clean_customer_zip_code_prefix != '' AND (LEN(clean_customer_zip_code_prefix) != 5 OR clean_customer_zip_code_prefix LIKE '%[^0-9]%')
            UNION ALL
            SELECT 'customer_state',           'Invalid length or format: expected 2 uppercase letters'  FROM #normalized_customers WHERE clean_customer_state != ''           AND (LEN(clean_customer_state) != 2 OR clean_customer_state LIKE '%[^A-Z]%')

            -- Duplicates Type A: same customer_id, identical content (hash match) — load artefact
            UNION ALL
            SELECT 'customer_id', 'Duplicate customer_id: identical content, deduplicated silently'
            FROM #normalized_customers n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_customers canon
                  WHERE canon.clean_customer_id = n.clean_customer_id
                    AND canon.rn               = 1
                    AND canon.row_hash         = n.row_hash
              )

            -- Duplicates Type B: same customer_id, conflicting content (hash mismatch) — data quality conflict
            UNION ALL
            SELECT 'customer_id', 'Duplicate customer_id: conflicting content — investigate before reload'
            FROM #normalized_customers n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_customers canon
                  WHERE canon.clean_customer_id = n.clean_customer_id
                    AND canon.rn               = 1
                    AND canon.row_hash         = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'customers', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same customer_id cannot be
        -- resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'customers'
              AND column_name = 'customer_id'
              AND issue LIKE 'Duplicate customer_id: conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate customer_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per customer_id) enter the MERGE as source.
        BEGIN TRANSACTION;
        MERGE cleansed.customers AS tgt
        USING (
            SELECT *
            FROM #normalized_customers
            WHERE rn = 1
              AND clean_customer_id IS NOT NULL AND clean_customer_id != '' AND clean_customer_id NOT LIKE '%[^0-9a-f]%'          AND LEN(clean_customer_id) = 32
              AND clean_customer_unique_id IS NOT NULL AND clean_customer_unique_id != '' AND clean_customer_unique_id NOT LIKE '%[^0-9a-f]%'   AND LEN(clean_customer_unique_id) = 32
              AND clean_customer_zip_code_prefix IS NOT NULL AND clean_customer_zip_code_prefix != '' AND clean_customer_zip_code_prefix NOT LIKE '%[^0-9]%' AND LEN(clean_customer_zip_code_prefix) = 5
              AND clean_customer_city IS NOT NULL            AND clean_customer_city != ''
              AND clean_customer_state IS NOT NULL           AND clean_customer_state != '' AND clean_customer_state NOT LIKE '%[^A-Z]%'           AND LEN(clean_customer_state) = 2
        ) AS src
        ON tgt.customer_id = src.clean_customer_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete.
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                customer_unique_id       = src.clean_customer_unique_id,
                customer_zip_code_prefix = src.clean_customer_zip_code_prefix,
                customer_city            = src.clean_customer_city,
                customer_state           = src.clean_customer_state,
                row_hash                 = src.row_hash,
                is_deleted               = 0,
                deleted_at               = NULL,
                updated_at               = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,              customer_unique_id,
                customer_zip_code_prefix, customer_city,
                customer_state,           row_hash,        updated_at
            )
            VALUES (
                src.clean_customer_id,              src.clean_customer_unique_id,
                src.clean_customer_zip_code_prefix, src.clean_customer_city,
                src.clean_customer_state,           src.row_hash, SYSUTCDATETIME()
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_customers';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_customers';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_customers',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
