USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_order_payments
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
            'CLEANSED',     'cleansed.sp_load_order_payments', 'cleansed.order_payments',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            row_id,
            order_id,
            payment_sequential,
            payment_type,
            payment_installments,
            payment_value,
            REPLACE(TRIM(order_id),    '"', '')                          AS clean_order_id,
            TRY_CAST(TRIM(payment_sequential)   AS INT)                  AS parsed_sequential,
            REPLACE(TRIM(payment_type), '"', '')                         AS clean_payment_type,
            TRY_CAST(TRIM(payment_installments) AS INT)                  AS parsed_installments,
            TRY_CAST(TRIM(payment_value)        AS DECIMAL(10,2))        AS parsed_value
        INTO #normalized_order_payments
        FROM raw.order_payments
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'order_id'             AS column_name, 'NULL value' AS issue FROM #normalized_order_payments WHERE clean_order_id IS NULL
            UNION ALL
            SELECT 'payment_sequential',   'NULL value'                         FROM #normalized_order_payments WHERE payment_sequential IS NULL
            UNION ALL
            SELECT 'payment_type',         'NULL value'                         FROM #normalized_order_payments WHERE clean_payment_type IS NULL
            UNION ALL
            SELECT 'payment_installments', 'NULL value'                         FROM #normalized_order_payments WHERE payment_installments IS NULL
            UNION ALL
            SELECT 'payment_value',        'NULL value'                         FROM #normalized_order_payments WHERE payment_value IS NULL

            -- Completeness: empty string checks
            UNION ALL
            SELECT 'order_id',     'Empty string after cleansing' FROM #normalized_order_payments WHERE clean_order_id = ''
            UNION ALL
            SELECT 'payment_type', 'Empty string after cleansing' FROM #normalized_order_payments WHERE clean_payment_type = ''

            -- Validity: format and length checks
            UNION ALL
            SELECT 'order_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_payments WHERE clean_order_id != '' AND (LEN(clean_order_id) != 32 OR clean_order_id LIKE '%[^0-9a-f]%')

            -- Validity: numeric parse failures
            UNION ALL
            SELECT 'payment_sequential',   'Invalid numeric format' FROM #normalized_order_payments WHERE payment_sequential   IS NOT NULL AND parsed_sequential   IS NULL
            UNION ALL
            SELECT 'payment_installments', 'Invalid numeric format' FROM #normalized_order_payments WHERE payment_installments IS NOT NULL AND parsed_installments IS NULL
            UNION ALL
            SELECT 'payment_value',        'Invalid decimal format' FROM #normalized_order_payments WHERE payment_value        IS NOT NULL AND parsed_value        IS NULL

            -- Validity: payment_type allowed values
            UNION ALL
            SELECT 'payment_type', 'Invalid value: not a recognized payment type'
            FROM #normalized_order_payments
            WHERE clean_payment_type != ''
              AND clean_payment_type NOT IN ('credit_card','boleto','voucher','debit_card','not_defined')

            -- Validity: sequential and installments must be >= 1
            UNION ALL
            SELECT 'payment_sequential',   'Invalid range: must be >= 1' FROM #normalized_order_payments WHERE parsed_sequential   IS NOT NULL AND parsed_sequential   < 1
            UNION ALL
            SELECT 'payment_installments', 'Invalid range: must be >= 1' FROM #normalized_order_payments WHERE parsed_installments IS NOT NULL AND parsed_installments < 1

            -- Validity: payment_value must be > 0 (vouchers exempt)
            UNION ALL
            SELECT 'payment_value', 'Invalid range: must be > 0'
            FROM #normalized_order_payments
            WHERE parsed_value IS NOT NULL AND parsed_value <= 0 AND payment_type <> 'voucher'

            -- Uniqueness: one row per duplicate occurrence so outer GROUP BY counts total
            UNION ALL
            SELECT 'order_id, payment_sequential', 'Duplicate (order_id, payment_sequential) in batch'
            FROM (SELECT COUNT(*) OVER (PARTITION BY order_id, payment_sequential) AS cnt FROM #normalized_order_payments) d
            WHERE cnt > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'order_payments', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id   = @batch_id
              AND table_name = 'order_payments'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate (order_id, payment_sequential) values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_order_id,
                parsed_sequential,
                clean_payment_type,
                parsed_installments,
                parsed_value,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,              '|',
                    CAST(parsed_sequential   AS NVARCHAR), '|',
                    clean_payment_type,          '|',
                    CAST(parsed_installments AS NVARCHAR), '|',
                    CAST(parsed_value        AS NVARCHAR)
                )) AS row_hash
            FROM #normalized_order_payments
        )
        MERGE cleansed.order_payments AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_order_id IS NOT NULL AND clean_order_id != '' AND clean_order_id NOT LIKE '%[^0-9a-f]%' AND LEN(clean_order_id) = 32
              AND parsed_sequential   IS NOT NULL
              AND clean_payment_type  IS NOT NULL AND clean_payment_type != ''
              AND parsed_installments IS NOT NULL
              AND parsed_value        IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id AND tgt.payment_sequential = src.parsed_sequential
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                payment_type         = src.clean_payment_type,
                payment_installments = src.parsed_installments,
                payment_value        = src.parsed_value,
                row_hash             = src.row_hash,
                is_deleted           = 0,
                deleted_at           = NULL,
                updated_at           = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,            payment_sequential,
                payment_type,        payment_installments,
                payment_value,       row_hash,              updated_at
            )
            VALUES (
                src.clean_order_id,  src.parsed_sequential,
                src.clean_payment_type, src.parsed_installments,
                src.parsed_value,    src.row_hash,          SYSUTCDATETIME()
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_payments';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_payments';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_order_payments',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
