USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_orders
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
            'CLEANSED',     'cleansed.sp_load_orders', 'cleansed.orders',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE.        --      'ranked'     — applies ROW_NUMBER() for deduplication on order_id.
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same order_id, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same order_id, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        --    ISNULL sentinel in ORDER BY pushes rows with NULL purchase timestamp behind valid
        --    rows, ensuring unparseable records are never selected as canonical.
        ;WITH normalized AS (
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                REPLACE(TRIM(order_id),       '"', '')  AS clean_order_id,
                REPLACE(TRIM(customer_id),    '"', '')  AS clean_customer_id,
                TRIM(order_status)                      AS clean_order_status,
                TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp),      120) AS parsed_order_purchase_ts,
                TRY_CONVERT(DATETIME2(0), TRIM(order_approved_at),             120) AS parsed_order_approved_at,
                TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_carrier_date),  120) AS parsed_order_delivered_carrier_date,
                TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) AS parsed_order_delivered_customer_date,
                TRY_CONVERT(DATETIME2(0), TRIM(order_estimated_delivery_date), 120) AS parsed_order_estimated_delivery_date
            FROM raw.orders
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,    '|', clean_customer_id,   '|', clean_order_status, '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_purchase_ts,              120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_approved_at,              120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_delivered_carrier_date,   120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_delivered_customer_date,  120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_order_estimated_delivery_date,  120), '')
                )) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_order_id
                    ORDER BY
                        ISNULL(parsed_order_purchase_ts, '9999-12-31'),
                        clean_customer_id
                ) AS rn
            FROM hashed
        )
        SELECT
            order_id, customer_id, order_status,
            order_purchase_timestamp, order_approved_at, order_delivered_carrier_date,
            order_delivered_customer_date, order_estimated_delivery_date,
            clean_order_id, clean_customer_id, clean_order_status,
            parsed_order_purchase_ts, parsed_order_approved_at, parsed_order_delivered_carrier_date,
            parsed_order_delivered_customer_date, parsed_order_estimated_delivery_date,
            row_hash, rn
        INTO #normalized_orders
        FROM ranked;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'order_id'                      AS column_name, 'NULL value' AS issue FROM #normalized_orders WHERE clean_order_id IS NULL
            UNION ALL
            SELECT 'customer_id',                   'NULL value'                         FROM #normalized_orders WHERE clean_customer_id IS NULL
            UNION ALL
            SELECT 'order_status',                  'NULL value'                         FROM #normalized_orders WHERE clean_order_status IS NULL
            UNION ALL
            SELECT 'order_purchase_timestamp',      'NULL value'                         FROM #normalized_orders WHERE order_purchase_timestamp IS NULL
            UNION ALL
            SELECT 'order_estimated_delivery_date', 'NULL value'                         FROM #normalized_orders WHERE order_estimated_delivery_date IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT 'order_id',     'Empty string after cleansing' FROM #normalized_orders WHERE clean_order_id = ''
            UNION ALL
            SELECT 'customer_id',  'Empty string after cleansing' FROM #normalized_orders WHERE clean_customer_id = ''
            UNION ALL
            SELECT 'order_status', 'Empty string after cleansing' FROM #normalized_orders WHERE clean_order_status = ''

            -- Validity: length and format checks (hex IDs)
            UNION ALL
            SELECT 'order_id',    'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_orders WHERE clean_order_id != ''   AND (LEN(clean_order_id) != 32 OR clean_order_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'customer_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_orders WHERE clean_customer_id != '' AND (LEN(clean_customer_id) != 32 OR clean_customer_id LIKE '%[^0-9a-f]%')

            -- Validity: order_status allowed values
            UNION ALL
            SELECT 'order_status', 'Invalid value: not a recognized order status'
            FROM #normalized_orders
            WHERE clean_order_status != ''
              AND clean_order_status NOT IN ('delivered','shipped','canceled','unavailable','invoiced','processing','approved','created')

            -- Validity: datetime format
            UNION ALL
            SELECT 'order_purchase_timestamp',      'Invalid datetime format' FROM #normalized_orders WHERE order_purchase_timestamp      IS NOT NULL AND parsed_order_purchase_ts IS NULL
            UNION ALL
            SELECT 'order_approved_at',             'Invalid datetime format' FROM #normalized_orders WHERE order_approved_at             IS NOT NULL AND parsed_order_approved_at IS NULL
            UNION ALL
            SELECT 'order_delivered_carrier_date',  'Invalid datetime format' FROM #normalized_orders WHERE order_delivered_carrier_date  IS NOT NULL AND parsed_order_delivered_carrier_date IS NULL
            UNION ALL
            SELECT 'order_delivered_customer_date', 'Invalid datetime format' FROM #normalized_orders WHERE order_delivered_customer_date IS NOT NULL AND parsed_order_delivered_customer_date IS NULL
            UNION ALL
            SELECT 'order_estimated_delivery_date', 'Invalid datetime format' FROM #normalized_orders WHERE order_estimated_delivery_date IS NOT NULL AND parsed_order_estimated_delivery_date IS NULL

            -- Validity: logical — customer date cannot precede purchase date
            UNION ALL
            SELECT 'order_delivered_customer_date', 'Delivered before purchase'
            FROM #normalized_orders
            WHERE parsed_order_delivered_customer_date IS NOT NULL
              AND parsed_order_purchase_ts             IS NOT NULL
              AND parsed_order_delivered_customer_date < parsed_order_purchase_ts

            -- Duplicates Type A: same order_id, identical content (hash match) — load artefact
            UNION ALL
            SELECT 'order_id', 'Duplicate order_id: identical content, deduplicated silently'
            FROM #normalized_orders n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_orders canon
                  WHERE canon.clean_order_id = n.clean_order_id
                    AND canon.rn             = 1
                    AND canon.row_hash       = n.row_hash
              )

            -- Duplicates Type B: same order_id, conflicting content (hash mismatch) — data quality conflict
            UNION ALL
            SELECT 'order_id', 'Duplicate order_id: conflicting content — investigate before reload'
            FROM #normalized_orders n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_orders canon
                  WHERE canon.clean_order_id = n.clean_order_id
                    AND canon.rn             = 1
                    AND canon.row_hash       = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'orders', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same order_id cannot be
        -- resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'orders'
              AND column_name = 'order_id'
              AND issue LIKE 'Duplicate order_id: conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate order_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per order_id) enter the MERGE as source.
        BEGIN TRANSACTION;
        MERGE cleansed.orders AS tgt
        USING (
            SELECT *
            FROM #normalized_orders
            WHERE rn = 1
              AND clean_order_id IS NOT NULL AND clean_order_id != '' AND clean_order_id NOT LIKE '%[^0-9a-f]%'     AND LEN(clean_order_id) = 32
              AND clean_customer_id IS NOT NULL AND clean_customer_id != '' AND clean_customer_id NOT LIKE '%[^0-9a-f]%'  AND LEN(clean_customer_id) = 32
              AND clean_order_status IS NOT NULL AND clean_order_status != ''
              AND parsed_order_purchase_ts IS NOT NULL
              AND parsed_order_estimated_delivery_date IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                customer_id                   = src.clean_customer_id,
                order_status                  = src.clean_order_status,
                order_purchase_timestamp      = src.parsed_order_purchase_ts,
                order_approved_at             = src.parsed_order_approved_at,
                order_delivered_carrier_date  = src.parsed_order_delivered_carrier_date,
                order_delivered_customer_date = src.parsed_order_delivered_customer_date,
                order_estimated_delivery_date = src.parsed_order_estimated_delivery_date,
                row_hash                      = src.row_hash,
                is_deleted                    = 0,
                deleted_at                    = NULL,
                updated_at                    = SYSUTCDATETIME()
        -- New row in current batch (source) that doesn't exist in cleansed (target)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,              customer_id,
                order_status,          order_purchase_timestamp,
                order_approved_at,     order_delivered_carrier_date,
                order_delivered_customer_date, order_estimated_delivery_date,
                row_hash,              updated_at
            )
            VALUES (
                src.clean_order_id,    src.clean_customer_id,
                src.clean_order_status, src.parsed_order_purchase_ts,
                src.parsed_order_approved_at, src.parsed_order_delivered_carrier_date,
                src.parsed_order_delivered_customer_date, src.parsed_order_estimated_delivery_date,
                src.row_hash,          SYSUTCDATETIME()
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_orders';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_orders';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_orders',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
