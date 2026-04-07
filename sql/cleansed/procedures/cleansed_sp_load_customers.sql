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
    DECLARE @start_time     DATETIME2     = SYSUTCDATETIME();
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
            'CLEANSED',     'cleansed.sp_load_customers', 'cleansed.customers',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            REPLACE(TRIM(customer_id),              '"', '') AS clean_customer_id,
            REPLACE(TRIM(customer_unique_id),        '"', '') AS clean_customer_unique_id,
            REPLACE(TRIM(customer_zip_code_prefix),  '"', '') AS clean_customer_zip_code_prefix,
            TRIM(customer_city)                               AS clean_customer_city,
            TRIM(customer_state)                              AS clean_customer_state
        INTO #normalized_customers
        FROM raw.customers
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT customer_id, 'customer_id'              AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_customers WHERE clean_customer_id IS NULL
            UNION ALL
            SELECT customer_id, 'customer_unique_id'       AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_customers WHERE clean_customer_unique_id IS NULL
            UNION ALL
            SELECT customer_id, 'customer_zip_code_prefix' AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_customers WHERE clean_customer_zip_code_prefix IS NULL
            UNION ALL
            SELECT customer_id, 'customer_city'            AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_customers WHERE clean_customer_city IS NULL
            UNION ALL
            SELECT customer_id, 'customer_state'           AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_customers WHERE clean_customer_state IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT customer_id, 'customer_id'              AS column_name, 'Empty string after cleansing' AS issue, customer_id              AS raw_value FROM #normalized_customers WHERE clean_customer_id = ''
            UNION ALL
            SELECT customer_id, 'customer_unique_id'       AS column_name, 'Empty string after cleansing' AS issue, customer_unique_id       AS raw_value FROM #normalized_customers WHERE clean_customer_unique_id = ''
            UNION ALL
            SELECT customer_id, 'customer_zip_code_prefix' AS column_name, 'Empty string after cleansing' AS issue, customer_zip_code_prefix  AS raw_value FROM #normalized_customers WHERE clean_customer_zip_code_prefix = ''
            UNION ALL
            SELECT customer_id, 'customer_city'            AS column_name, 'Empty string after cleansing' AS issue, customer_city            AS raw_value FROM #normalized_customers WHERE clean_customer_city = ''
            UNION ALL
            SELECT customer_id, 'customer_state'           AS column_name, 'Empty string after cleansing' AS issue, customer_state           AS raw_value FROM #normalized_customers WHERE clean_customer_state = ''

            -- Validity: length checks (empty strings excluded to avoid double-reporting)
            UNION ALL
            SELECT customer_id, 'customer_id'              AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_customer_id) AS NVARCHAR)              AS issue, customer_id              AS raw_value FROM #normalized_customers WHERE clean_customer_id != ''              AND LEN(clean_customer_id) != 32
            UNION ALL
            SELECT customer_id, 'customer_unique_id'       AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_customer_unique_id) AS NVARCHAR)       AS issue, customer_unique_id       AS raw_value FROM #normalized_customers WHERE clean_customer_unique_id != ''       AND LEN(clean_customer_unique_id) != 32
            UNION ALL
            SELECT customer_id, 'customer_zip_code_prefix' AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_customer_zip_code_prefix) AS NVARCHAR) AS issue, customer_zip_code_prefix  AS raw_value FROM #normalized_customers WHERE clean_customer_zip_code_prefix != '' AND LEN(clean_customer_zip_code_prefix) != 5
            UNION ALL
            SELECT customer_id, 'customer_state'           AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_customer_state) AS NVARCHAR)           AS issue, customer_state           AS raw_value FROM #normalized_customers WHERE clean_customer_state != ''           AND LEN(clean_customer_state) != 2

            -- Validity: format checks (applied only to rows that passed length validation)
            -- customer_id and customer_unique_id are MD5 hashes: 32 lowercase hex chars
            UNION ALL
            SELECT customer_id, 'customer_id'              AS column_name, 'Invalid format: expected 32-char lowercase hex' AS issue, customer_id             AS raw_value FROM #normalized_customers WHERE LEN(clean_customer_id) = 32              AND clean_customer_id LIKE '%[^0-9a-f]%'
            UNION ALL
            SELECT customer_id, 'customer_unique_id'       AS column_name, 'Invalid format: expected 32-char lowercase hex' AS issue, customer_unique_id      AS raw_value FROM #normalized_customers WHERE LEN(clean_customer_unique_id) = 32       AND clean_customer_unique_id LIKE '%[^0-9a-f]%'
            -- customer_zip_code_prefix: Brazilian CEP prefix — 5 numeric digits
            UNION ALL
            SELECT customer_id, 'customer_zip_code_prefix' AS column_name, 'Invalid format: expected 5 numeric digits'      AS issue, customer_zip_code_prefix AS raw_value FROM #normalized_customers WHERE LEN(clean_customer_zip_code_prefix) = 5  AND clean_customer_zip_code_prefix LIKE '%[^0-9]%'
            -- customer_state: Brazilian state/district code — 2 uppercase letters
            UNION ALL
            SELECT customer_id, 'customer_state'           AS column_name, 'Invalid format: expected 2 uppercase letters'   AS issue, customer_state          AS raw_value FROM #normalized_customers WHERE LEN(clean_customer_state) = 2            AND clean_customer_state LIKE '%[^A-Z]%'

            -- Uniqueness: duplicate customer_id within batch
            UNION ALL
            SELECT
                customer_id,
                'customer_id'                                                                      AS column_name,
                'Duplicate customer_id in batch: ' + CAST(COUNT(*) AS NVARCHAR) + ' occurrences'  AS issue,
                customer_id                                                                        AS raw_value
            FROM #normalized_customers
            GROUP BY customer_id
            HAVING COUNT(*) > 1
        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, raw_key, column_name, issue, raw_value)
        SELECT @batch_id, @job_run_id, 'customers', customer_id, column_name, issue, raw_value
        FROM dq_checks;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'customers'
              AND column_name = 'customer_id'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate customer_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_customer_id,
                clean_customer_unique_id,
                clean_customer_zip_code_prefix,
                clean_customer_city,
                clean_customer_state,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_customer_id,              '|',
                    clean_customer_unique_id,        '|',
                    clean_customer_zip_code_prefix,  '|',
                    clean_customer_city,             '|',
                    clean_customer_state
                )) AS row_hash
            FROM #normalized_customers
        )
        MERGE cleansed.customers AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_customer_id IS NOT NULL
              AND clean_customer_unique_id IS NOT NULL
              AND clean_customer_zip_code_prefix IS NOT NULL
              AND clean_customer_city IS NOT NULL
              AND clean_customer_state IS NOT NULL
              AND LEN(clean_customer_id) = 32
              AND LEN(clean_customer_unique_id) = 32
              AND LEN(clean_customer_zip_code_prefix) = 5
              AND LEN(clean_customer_city) > 0
              AND LEN(clean_customer_state) = 2
        ) AS src
        ON tgt.customer_id = src.clean_customer_id
        -- Data changed or row is reactivating after a soft delete
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
        -- Row exists in cleansed but not in current batch — source no longer contains it
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
