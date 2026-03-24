CREATE OR ALTER PROCEDURE cleansed.sp_load_customers
AS
BEGIN
    SET NOCOUNT ON;

    -- --------------------------------------------------------
    -- 1. Truncate
    -- --------------------------------------------------------
    TRUNCATE TABLE cleansed.customers;


    -- --------------------------------------------------------
    -- 2. Error Logging
    -- --------------------------------------------------------

    -- NULL customer_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_id', 'NULL value', NULL
    FROM raw.customers
    WHERE customer_id IS NULL;

    -- NULL customer_unique_id
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_unique_id', 'NULL value', NULL
    FROM raw.customers
    WHERE customer_unique_id IS NULL;

    -- NULL customer_zip_code_prefix
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_zip_code_prefix', 'NULL value', NULL
    FROM raw.customers
    WHERE customer_zip_code_prefix IS NULL;

    -- NULL customer_state
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_state', 'NULL value', NULL
    FROM raw.customers
    WHERE customer_state IS NULL;

    -- NULL customer_city
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_city', 'NULL value', NULL
    FROM raw.customers
    WHERE customer_city IS NULL;

    -- Empty string customer_id after cleansing
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_id', 'Empty string after cleansing', customer_id
    FROM raw.customers
    WHERE TRIM(REPLACE(customer_id, '"', '')) = '';

    -- Empty string customer_unique_id after cleansing
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_unique_id', 'Empty string after cleansing', customer_unique_id
    FROM raw.customers
    WHERE TRIM(REPLACE(customer_unique_id, '"', '')) = '';

    -- Empty string customer_city after cleansing
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_city', 'Empty string after cleansing', customer_city
    FROM raw.customers
    WHERE TRIM(customer_city) = '';

    -- Empty string customer_state after cleansing
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_state', 'Empty string after cleansing', customer_state
    FROM raw.customers
    WHERE TRIM(customer_state) = '';

    -- Empty string customer_city after cleansing
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_city', 'Empty string after cleansing', customer_city
    FROM raw.customers
    WHERE TRIM(customer_city) = ''; 

    -- Invalid customer_id length (should be 32 after cleansing)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_id', 'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_id, '"', ''))) AS NVARCHAR), customer_id
    FROM raw.customers
    WHERE LEN(TRIM(REPLACE(customer_id, '"', ''))) != 32
    AND customer_id IS NOT NULL;

    -- Invalid customer_unique_id length (should be 32 after cleansing)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_unique_id', 'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_unique_id, '"', ''))) AS NVARCHAR), customer_unique_id
    FROM raw.customers
    WHERE LEN(TRIM(REPLACE(customer_unique_id, '"', ''))) != 32
    AND customer_unique_id IS NOT NULL;

    -- Invalid zip code length (should be 5 after cleansing)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_zip_code_prefix', 'Invalid length after cleansing: ' + CAST(LEN(TRIM(REPLACE(customer_zip_code_prefix, '"', ''))) AS NVARCHAR), customer_zip_code_prefix
    FROM raw.customers
    WHERE LEN(TRIM(REPLACE(customer_zip_code_prefix, '"', ''))) != 5
    AND customer_zip_code_prefix IS NOT NULL;

    -- Invalid state length (should be 2)
    INSERT INTO cleansed.error_log (table_name, raw_key, column_name, issue, raw_value)
    SELECT 'customers', customer_id, 'customer_state', 'Invalid length: ' + CAST(LEN(TRIM(customer_state)) AS NVARCHAR), customer_state
    FROM raw.customers
    WHERE LEN(TRIM(customer_state)) != 2
    AND customer_state IS NOT NULL;


    -- --------------------------------------------------------
    -- 3. Load cleansed data
    -- --------------------------------------------------------
    INSERT INTO cleansed.customers (
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    )
    SELECT
        REPLACE(TRIM(customer_id),              '"', ''),
        REPLACE(TRIM(customer_unique_id),       '"', ''),
        REPLACE(TRIM(customer_zip_code_prefix), '"', ''),
        TRIM(customer_city),
        TRIM(customer_state)
    FROM raw.customers;

    DECLARE @error_count INT;
    SELECT @error_count = COUNT(*) FROM cleansed.error_log WHERE table_name = 'customers';

    PRINT 'cleansed.customers loaded: ' + CAST(@@ROWCOUNT AS NVARCHAR) + ' rows';
    PRINT 'Errors logged: ' + CAST(@error_count AS NVARCHAR);
END;
GO
