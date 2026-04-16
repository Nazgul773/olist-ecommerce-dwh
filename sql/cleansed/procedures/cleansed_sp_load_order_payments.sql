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
            'CLEANSED',     'cleansed.sp_load_order_payments', 'cleansed.order_payments',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE.
        --      'ranked'     — applies ROW_NUMBER() for deduplication on (order_id, payment_sequential).
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same composite key, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same composite key, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        ;WITH normalized AS (
            SELECT
                order_id,
                payment_sequential,
                payment_type,
                payment_installments,
                payment_value,
                REPLACE(TRIM(order_id),    '"', '')                   AS clean_order_id,
                TRY_CAST(TRIM(payment_sequential)   AS INT)           AS parsed_sequential,
                REPLACE(TRIM(payment_type), '"', '')                  AS clean_payment_type,
                TRY_CAST(TRIM(payment_installments) AS INT)           AS parsed_installments,
                TRY_CAST(TRIM(payment_value)        AS DECIMAL(10,2)) AS parsed_value
            FROM raw.order_payments
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,                               '|',
                    CAST(parsed_sequential   AS NVARCHAR),        '|',
                    clean_payment_type,                           '|',
                    CAST(parsed_installments AS NVARCHAR),        '|',
                    CAST(parsed_value        AS NVARCHAR)
                )) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_order_id, parsed_sequential
                    ORDER BY clean_payment_type
                ) AS rn
            FROM hashed
        )
        SELECT
            order_id, payment_sequential, payment_type, payment_installments, payment_value,
            clean_order_id, parsed_sequential, clean_payment_type, parsed_installments, parsed_value,
            row_hash, rn
        INTO #normalized_order_payments
        FROM ranked;

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
            WHERE parsed_value IS NOT NULL AND parsed_value <= 0 AND clean_payment_type <> 'voucher'

            -- Duplicates Type A: same (order_id, payment_sequential), identical content — load artefact
            UNION ALL
            SELECT 'order_id, payment_sequential', 'Duplicate (order_id, payment_sequential): identical content, deduplicated silently'
            FROM #normalized_order_payments n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_order_payments canon
                  WHERE canon.clean_order_id      = n.clean_order_id
                    AND canon.parsed_sequential   = n.parsed_sequential
                    AND canon.rn                  = 1
                    AND canon.row_hash            = n.row_hash
              )

            -- Duplicates Type B: same (order_id, payment_sequential), conflicting content — data quality conflict
            UNION ALL
            SELECT 'order_id, payment_sequential', 'Duplicate (order_id, payment_sequential): conflicting content — investigate before reload'
            FROM #normalized_order_payments n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_order_payments canon
                  WHERE canon.clean_order_id      = n.clean_order_id
                    AND canon.parsed_sequential   = n.parsed_sequential
                    AND canon.rn                  = 1
                    AND canon.row_hash            = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'order_payments', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same (order_id, payment_sequential)
        -- cannot be resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'order_payments'
              AND column_name = 'order_id, payment_sequential'
              AND issue LIKE 'Duplicate (order_id, payment_sequential): conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate (order_id, payment_sequential) values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per composite key) enter the MERGE as source.
        BEGIN TRANSACTION;
        MERGE cleansed.order_payments AS tgt
        USING (
            SELECT *
            FROM #normalized_order_payments
            WHERE rn = 1
              AND clean_order_id IS NOT NULL AND clean_order_id != '' AND clean_order_id NOT LIKE '%[^0-9a-f]%' AND LEN(clean_order_id) = 32
              AND parsed_sequential   IS NOT NULL
              AND clean_payment_type  IS NOT NULL AND clean_payment_type != ''
              AND parsed_installments IS NOT NULL
              AND parsed_value        IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id AND tgt.payment_sequential = src.parsed_sequential
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                payment_type         = src.clean_payment_type,
                payment_installments = src.parsed_installments,
                payment_value        = src.parsed_value,
                row_hash             = src.row_hash,
                is_deleted           = 0,
                deleted_at           = NULL,
                updated_at           = SYSUTCDATETIME()
        -- New row in current batch (source) that doesn't exist in cleansed (target)
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
        -- Row exists in cleansed (target) but not in current batch (source) — source no longer contains it
        -- Soft delete by marking is_deleted = 1 and setting deleted_at for historical tracking, instead of hard deleting.
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
