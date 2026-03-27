USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_customers
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

        -- NULL customer_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_id', 'NULL value', NULL
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND customer_id IS NULL;

        -- NULL customer_unique_id
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_unique_id', 'NULL value', NULL
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND customer_unique_id IS NULL;

        -- NULL customer_zip_code_prefix
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_zip_code_prefix', 'NULL value', NULL
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND customer_zip_code_prefix IS NULL;

        -- NULL customer_state
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_state', 'NULL value', NULL
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND customer_state IS NULL;

        -- NULL customer_city
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_city', 'NULL value', NULL
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND customer_city IS NULL;

        -- Empty string customer_id after cleansing
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_id', 'Empty string after cleansing', customer_id
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND TRIM(REPLACE(customer_id, '"', '')) = '';

        -- Empty string customer_unique_id after cleansing
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_unique_id', 'Empty string after cleansing', customer_unique_id
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND TRIM(REPLACE(customer_unique_id, '"', '')) = '';

        -- Empty string customer_city after cleansing
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_city', 'Empty string after cleansing', customer_city
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND TRIM(customer_city) = '';

        -- Empty string customer_state after cleansing
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_state', 'Empty string after cleansing', customer_state
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND TRIM(customer_state) = '';

        -- Invalid customer_id length (should be 32 after cleansing)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_id',
            'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_id, '"', ''))) AS NVARCHAR),
            customer_id
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND LEN(TRIM(REPLACE(customer_id, '"', ''))) != 32
          AND customer_id IS NOT NULL;

        -- Invalid customer_unique_id length (should be 32 after cleansing)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_unique_id',
            'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_unique_id, '"', ''))) AS NVARCHAR),
            customer_unique_id
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND LEN(TRIM(REPLACE(customer_unique_id, '"', ''))) != 32
          AND customer_unique_id IS NOT NULL;

        -- Invalid zip code length (should be 5 after cleansing)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_zip_code_prefix',
            'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_zip_code_prefix, '"', ''))) AS NVARCHAR),
            customer_zip_code_prefix
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND LEN(TRIM(REPLACE(customer_zip_code_prefix, '"', ''))) != 5
          AND customer_zip_code_prefix IS NOT NULL;

        -- Invalid state length (should be 2)
        INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
        SELECT 'customers', customer_id, 'customer_state',
            'Invalid length: ' + CAST(LEN(TRIM(customer_state)) AS NVARCHAR),
            customer_state
        FROM raw.customers
        WHERE batch_id = @batch_id
          AND LEN(TRIM(customer_state)) != 2
          AND customer_state IS NOT NULL;

        -- --------------------------------------------------------
        -- 2. Incremental upsert into cleansed.customers (MERGE)
        --    row_hash controls whether a row gets updated or not.
        -- --------------------------------------------------------

        ;WITH normalized AS (
            SELECT
                REPLACE(TRIM(customer_id), '"', '')            AS customer_id,
                REPLACE(TRIM(customer_unique_id), '"', '')     AS customer_unique_id,
                REPLACE(TRIM(customer_zip_code_prefix), '"', '') AS customer_zip_code_prefix,
                TRIM(customer_city)         AS customer_city,
                TRIM(customer_state)         AS customer_state
            FROM raw.customers
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                HASHBYTES('SHA2_256', CONCAT(
                    customer_id, '|',
                    customer_unique_id, '|',
                    customer_zip_code_prefix, '|',
                    customer_city, '|',
                    customer_state
                )) AS row_hash
            FROM normalized
        )
        MERGE cleansed.customers AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE customer_id IS NOT NULL
            AND customer_unique_id IS NOT NULL
            AND LEN(customer_id) = 32
            AND LEN(customer_unique_id) = 32
            AND customer_city IS NOT NULL
            AND LEN(customer_city) > 0
            AND LEN(customer_zip_code_prefix) = 5
            AND LEN(customer_state) = 2
        ) AS src
        ON tgt.customer_id = src.customer_id
        WHEN MATCHED AND tgt.row_hash <> src.row_hash THEN
            UPDATE SET
                customer_unique_id = src.customer_unique_id,
                customer_zip_code_prefix = src.customer_zip_code_prefix,
                customer_city = src.customer_city,
                customer_state = src.customer_state,
                row_hash = src.row_hash,
                updated_at = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,
                customer_unique_id,
                customer_zip_code_prefix,
                customer_city,
                customer_state,
                row_hash,
                updated_at
            )
            VALUES (
                src.customer_id,
                src.customer_unique_id,
                src.customer_zip_code_prefix,
                src.customer_city,
                src.customer_state,
                src.row_hash,
                GETDATE()
            );

        SET @merge_rowcount = @@ROWCOUNT;

        SELECT @error_count = COUNT(*)
        FROM cleansed.error_log
        WHERE table_name = 'customers'
            AND batch_id = @batch_id;

        PRINT 'cleansed.customers merged (insert/update): ' + CAST(@merge_rowcount AS NVARCHAR) + ' rows';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR(36));
        PRINT 'Errors logged: ' + CAST(@error_count AS NVARCHAR);

    END TRY
    BEGIN CATCH
        PRINT 'Error in cleansed.sp_load_customers: ' + ERROR_MESSAGE();
        THROW;
    END CATCH

END;
GO
