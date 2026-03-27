USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_orders
    @batch_id UNIQUEIDENTIFIER
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @merge_rowcount INT;
    DECLARE @error_count    INT;

    BEGIN TRY
        -- --------------------------------------------------------
        -- 1. Error logging (DQ checks) - based on current batch
        -- --------------------------------------------------------

        -- NULL order_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_id', 'NULL value', NULL
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND order_id IS NULL;

        -- NULL customer_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'customer_id', 'NULL value', NULL
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND customer_id IS NULL;

        -- NULL order_status
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_status', 'NULL value', NULL
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND order_status IS NULL;

        -- NULL order_purchase_timestamp
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_purchase_timestamp', 'NULL value', NULL
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND order_purchase_timestamp IS NULL;

        -- NULL order_estimated_delivery_date
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_estimated_delivery_date', 'NULL value', NULL
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND order_estimated_delivery_date IS NULL;

        -- Empty string after cleansing: order_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_id', 'Empty string after cleansing', order_id
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRIM(REPLACE(order_id, '"', '')) = '';

        -- Empty string after cleansing: customer_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'customer_id', 'Empty string after cleansing', customer_id
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRIM(REPLACE(customer_id, '"', '')) = '';

        -- Empty string after cleansing: order_status
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_status', 'Empty string after cleansing', order_status
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRIM(order_status) = '';

        -- Invalid length: order_id (should be 32)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_id',
            'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(order_id, '"', ''))) AS NVARCHAR),
            order_id
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND LEN(TRIM(REPLACE(order_id, '"', ''))) != 32
        AND order_id IS NOT NULL;

        -- Invalid length: customer_id (should be 32)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'customer_id',
            'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_id, '"', ''))) AS NVARCHAR),
            customer_id
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND LEN(TRIM(REPLACE(customer_id, '"', ''))) != 32
        AND customer_id IS NOT NULL;

        -- Invalid datetime format: order_purchase_timestamp
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_purchase_timestamp', 'Invalid datetime format', order_purchase_timestamp
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_purchase_timestamp, 120) IS NULL
        AND order_purchase_timestamp IS NOT NULL;

        -- Invalid datetime format: order_approved_at
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_approved_at', 'Invalid datetime format', order_approved_at
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_approved_at, 120) IS NULL
        AND order_approved_at IS NOT NULL;

        -- Invalid datetime format: order_delivered_carrier_date
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_delivered_carrier_date', 'Invalid datetime format', order_delivered_carrier_date
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_delivered_carrier_date, 120) IS NULL
        AND order_delivered_carrier_date IS NOT NULL;

        -- Invalid datetime format: order_delivered_customer_date
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_delivered_customer_date', 'Invalid datetime format', order_delivered_customer_date
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_delivered_customer_date, 120) IS NULL
        AND order_delivered_customer_date IS NOT NULL;

        -- Invalid datetime format: order_estimated_delivery_date
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_estimated_delivery_date', 'Invalid datetime format', order_estimated_delivery_date
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_estimated_delivery_date, 120) IS NULL
        AND order_estimated_delivery_date IS NOT NULL;

        -- Logical check: delivered_customer_date before purchase_timestamp
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'orders', order_id, 'order_delivered_customer_date',
            'Delivered before purchase',
            order_delivered_customer_date
        FROM raw.orders
        WHERE batch_id = @batch_id
        AND TRY_CONVERT(DATETIME, order_delivered_customer_date, 120) < TRY_CONVERT(DATETIME, order_purchase_timestamp, 120)
        AND order_delivered_customer_date IS NOT NULL
        AND order_purchase_timestamp IS NOT NULL;

        -- --------------------------------------------------------
        -- 2. Incremental upsert into cleansed.orders (MERGE)
        --    row_hash controls whether a row gets updated or not.
        -- --------------------------------------------------------

        ;WITH normalized AS (
            SELECT
                REPLACE(TRIM(order_id), '"', '')        AS order_id,
                REPLACE(TRIM(customer_id), '"', '')     AS customer_id,
                TRIM(order_status)                     AS order_status,
                TRY_CONVERT(DATETIME, order_purchase_timestamp, 120) AS order_purchase_timestamp,
                TRY_CONVERT(DATETIME, order_approved_at, 120)        AS order_approved_at,
                TRY_CONVERT(DATETIME, order_delivered_carrier_date, 120)  AS order_delivered_carrier_date,
                TRY_CONVERT(DATETIME, order_delivered_customer_date, 120) AS order_delivered_customer_date,
                TRY_CONVERT(DATETIME, order_estimated_delivery_date, 120) AS order_estimated_delivery_date
            FROM raw.orders
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                HASHBYTES('SHA2_256', CONCAT(
                    order_id, '|',
                    customer_id, '|',
                    order_status, '|',
                    COALESCE(CONVERT(NVARCHAR(30), order_purchase_timestamp, 126), ''), '|',
                    COALESCE(CONVERT(NVARCHAR(30), order_approved_at, 126), ''), '|',
                    COALESCE(CONVERT(NVARCHAR(30), order_delivered_carrier_date, 126), ''), '|',
                    COALESCE(CONVERT(NVARCHAR(30), order_delivered_customer_date, 126), ''), '|',
                    COALESCE(CONVERT(NVARCHAR(30), order_estimated_delivery_date, 126), '')
                )) AS row_hash
            FROM normalized
        )
        MERGE cleansed.orders AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE order_id IS NOT NULL
            AND customer_id IS NOT NULL
            AND order_status IS NOT NULL
            AND LEN(order_id) = 32
            AND LEN(customer_id) = 32
            AND LEN(order_status) > 0
            AND LEN(order_status) <= 25
            AND order_purchase_timestamp IS NOT NULL
            AND order_estimated_delivery_date IS NOT NULL
            AND (order_delivered_customer_date IS NULL OR order_delivered_customer_date >= order_purchase_timestamp)
        ) AS src
        ON tgt.order_id = src.order_id
        WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
            UPDATE SET
                customer_id = src.customer_id,
                order_status = src.order_status,
                order_purchase_timestamp = src.order_purchase_timestamp,
                order_approved_at = src.order_approved_at,
                order_delivered_carrier_date = src.order_delivered_carrier_date,
                order_delivered_customer_date = src.order_delivered_customer_date,
                order_estimated_delivery_date = src.order_estimated_delivery_date,
                row_hash = src.row_hash,
                updated_at = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                order_id,
                customer_id,
                order_status,
                order_purchase_timestamp,
                order_approved_at,
                order_delivered_carrier_date,
                order_delivered_customer_date,
                order_estimated_delivery_date,
                row_hash,
                updated_at
            )
            VALUES (
                src.order_id,
                src.customer_id,
                src.order_status,
                src.order_purchase_timestamp,
                src.order_approved_at,
                src.order_delivered_carrier_date,
                src.order_delivered_customer_date,
                src.order_estimated_delivery_date,
                src.row_hash,
                GETDATE()
            );

        SET @merge_rowcount = @@ROWCOUNT;

        SELECT @error_count = COUNT(*)
        FROM cleansed.error_log
        WHERE table_name = 'orders'
            AND batch_id = @batch_id;

        PRINT 'cleansed.orders merged (insert/update): ' + CAST(@merge_rowcount AS NVARCHAR) + ' rows';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR(36));
        PRINT 'Errors logged: ' + CAST(@error_count AS NVARCHAR);

    END TRY
    BEGIN CATCH
        PRINT 'Error in cleansed.sp_load_orders: ' + ERROR_MESSAGE();
        THROW;
    END CATCH

    PRINT '--------------------------------------------------------'

END;
GO
