USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_order_items
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
            'CLEANSED',     'cleansed.sp_load_order_items', 'cleansed.order_items',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        SELECT
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            REPLACE(TRIM(order_id),    '"', '')  AS clean_order_id,
            TRIM(order_item_id)                  AS clean_order_item_id,
            REPLACE(TRIM(product_id),  '"', '')  AS clean_product_id,
            REPLACE(TRIM(seller_id),   '"', '')  AS clean_seller_id,
            TRY_CONVERT(DATETIME2(0),  TRIM(shipping_limit_date), 120) AS parsed_shipping_date,
            TRY_CONVERT(DECIMAL(10,2), TRIM(price))                    AS parsed_price,
            TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value))            AS parsed_freight_value
        INTO #normalized_order_items
        FROM raw.order_items
        WHERE batch_id = @batch_id;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT order_id, 'order_id'            AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE clean_order_id IS NULL
            UNION ALL
            SELECT order_id, 'order_item_id'        AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE clean_order_item_id IS NULL
            UNION ALL
            SELECT order_id, 'product_id'           AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE clean_product_id IS NULL
            UNION ALL
            SELECT order_id, 'seller_id'            AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE clean_seller_id IS NULL
            UNION ALL
            SELECT order_id, 'shipping_limit_date'  AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE shipping_limit_date IS NULL
            UNION ALL
            SELECT order_id, 'price'                AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE price IS NULL
            UNION ALL
            SELECT order_id, 'freight_value'        AS column_name, 'NULL value' AS issue, CAST(NULL AS NVARCHAR(MAX)) AS raw_value FROM #normalized_order_items WHERE freight_value IS NULL

            -- Completeness: empty string checks after cleansing
            UNION ALL
            SELECT order_id, 'order_id'       AS column_name, 'Empty string after cleansing' AS issue, order_id       AS raw_value FROM #normalized_order_items WHERE clean_order_id = ''
            UNION ALL
            SELECT order_id, 'order_item_id'  AS column_name, 'Empty string after cleansing' AS issue, order_item_id  AS raw_value FROM #normalized_order_items WHERE clean_order_item_id = ''
            UNION ALL
            SELECT order_id, 'product_id'     AS column_name, 'Empty string after cleansing' AS issue, product_id     AS raw_value FROM #normalized_order_items WHERE clean_product_id = ''
            UNION ALL
            SELECT order_id, 'seller_id'      AS column_name, 'Empty string after cleansing' AS issue, seller_id      AS raw_value FROM #normalized_order_items WHERE clean_seller_id = ''

            -- Validity: length checks (hex IDs)
            UNION ALL
            SELECT order_id, 'order_id'   AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_order_id) AS NVARCHAR)   AS issue, order_id   AS raw_value FROM #normalized_order_items WHERE clean_order_id != ''   AND LEN(clean_order_id) != 32
            UNION ALL
            SELECT order_id, 'product_id' AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_product_id) AS NVARCHAR) AS issue, product_id AS raw_value FROM #normalized_order_items WHERE clean_product_id != '' AND LEN(clean_product_id) != 32
            UNION ALL
            SELECT order_id, 'seller_id'  AS column_name, 'Invalid length after cleansing: ' + CAST(LEN(clean_seller_id) AS NVARCHAR)  AS issue, seller_id  AS raw_value FROM #normalized_order_items WHERE clean_seller_id != ''  AND LEN(clean_seller_id) != 32

            -- Validity: format checks (hex IDs)
            UNION ALL
            SELECT order_id, 'order_id'   AS column_name, 'Invalid format: expected 32-char lowercase hex' AS issue, order_id   AS raw_value FROM #normalized_order_items WHERE LEN(clean_order_id) = 32   AND clean_order_id LIKE '%[^0-9a-f]%'
            UNION ALL
            SELECT order_id, 'product_id' AS column_name, 'Invalid format: expected 32-char lowercase hex' AS issue, product_id AS raw_value FROM #normalized_order_items WHERE LEN(clean_product_id) = 32 AND clean_product_id LIKE '%[^0-9a-f]%'
            UNION ALL
            SELECT order_id, 'seller_id'  AS column_name, 'Invalid format: expected 32-char lowercase hex' AS issue, seller_id  AS raw_value FROM #normalized_order_items WHERE LEN(clean_seller_id) = 32  AND clean_seller_id LIKE '%[^0-9a-f]%'

            -- Validity: order_item_id must be numeric (sequential integer per order)
            UNION ALL
            SELECT order_id, 'order_item_id' AS column_name, 'Invalid format: expected numeric value' AS issue, order_item_id AS raw_value
            FROM #normalized_order_items
            WHERE clean_order_item_id != '' AND clean_order_item_id LIKE '%[^0-9]%'

            -- Validity: datetime format
            UNION ALL
            SELECT order_id, 'shipping_limit_date' AS column_name, 'Invalid datetime format' AS issue, shipping_limit_date AS raw_value
            FROM #normalized_order_items
            WHERE shipping_limit_date IS NOT NULL AND parsed_shipping_date IS NULL

            -- Validity: decimal format
            UNION ALL
            SELECT order_id, 'price'         AS column_name, 'Invalid decimal format' AS issue, price         AS raw_value FROM #normalized_order_items WHERE price IS NOT NULL         AND parsed_price IS NULL
            UNION ALL
            SELECT order_id, 'freight_value' AS column_name, 'Invalid decimal format' AS issue, freight_value AS raw_value FROM #normalized_order_items WHERE freight_value IS NOT NULL AND parsed_freight_value IS NULL

            -- Validity: price must be positive (free items don't exist in this dataset)
            UNION ALL
            SELECT order_id, 'price' AS column_name, 'Invalid range: price must be > 0' AS issue, price AS raw_value
            FROM #normalized_order_items
            WHERE parsed_price IS NOT NULL AND parsed_price <= 0

            -- Uniqueness: duplicate composite key (order_id, order_item_id) within batch
            UNION ALL
            SELECT
                order_id,
                'order_id, order_item_id'                                                                              AS column_name,
                'Duplicate (order_id, order_item_id) in batch: ' + CAST(COUNT(*) AS NVARCHAR) + ' occurrences'        AS issue,
                order_item_id                                                                                          AS raw_value
            FROM #normalized_order_items
            GROUP BY order_id, order_item_id
            HAVING COUNT(*) > 1
        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, raw_key, column_name, issue, raw_value)
        SELECT @batch_id, @job_run_id, 'order_items', order_id, column_name, issue, raw_value
        FROM dq_checks;

        -- Abort if duplicates were detected.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id   = @batch_id
              AND table_name = 'order_items'
              AND issue LIKE 'Duplicate%'
        )
            THROW 50004, 'Duplicate (order_id, order_item_id) values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_order_id,
                clean_order_item_id,
                clean_product_id,
                clean_seller_id,
                parsed_shipping_date,
                parsed_price,
                parsed_freight_value,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_order_id,      '|', clean_order_item_id, '|',
                    clean_product_id,    '|', clean_seller_id,     '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_shipping_date, 120), ''), '|',
                    ISNULL(CONVERT(NVARCHAR(20), parsed_price),        ''),       '|',
                    ISNULL(CONVERT(NVARCHAR(20), parsed_freight_value),'')
                )) AS row_hash
            FROM #normalized_order_items
        )
        MERGE cleansed.order_items AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_order_id IS NOT NULL
              AND clean_order_item_id IS NOT NULL
              AND clean_product_id IS NOT NULL
              AND clean_seller_id IS NOT NULL
              AND LEN(clean_order_id) = 32
              AND LEN(clean_product_id) = 32
              AND LEN(clean_seller_id) = 32
              AND parsed_shipping_date IS NOT NULL
              AND parsed_price IS NOT NULL
              AND parsed_price > 0
              AND parsed_freight_value IS NOT NULL
        ) AS src
        ON tgt.order_id = src.clean_order_id AND tgt.order_item_id = src.clean_order_item_id
        -- Data changed or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                product_id          = src.clean_product_id,
                seller_id           = src.clean_seller_id,
                shipping_limit_date = src.parsed_shipping_date,
                price               = src.parsed_price,
                freight_value       = src.parsed_freight_value,
                row_hash            = src.row_hash,
                is_deleted          = 0,
                deleted_at          = NULL,
                updated_at          = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,            order_item_id,
                product_id,          seller_id,
                shipping_limit_date, price,
                freight_value,       row_hash,        updated_at
            )
            VALUES (
                src.clean_order_id,  src.clean_order_item_id,
                src.clean_product_id, src.clean_seller_id,
                src.parsed_shipping_date, src.parsed_price,
                src.parsed_freight_value, src.row_hash, SYSUTCDATETIME()
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_items';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_items';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_order_items',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
