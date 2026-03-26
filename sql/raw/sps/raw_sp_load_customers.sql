USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE raw.sp_load_customers
    @file_path  NVARCHAR(500),
    @file_name  NVARCHAR(255)
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql  NVARCHAR(MAX);
    DECLARE @rows INT;

    BEGIN TRY
        -- --------------------------------------------------------
        -- 1. Create temp table
        -- --------------------------------------------------------
        DROP TABLE IF EXISTS #tmp_customers;

        CREATE TABLE #tmp_customers (
            [customer_id]               NVARCHAR(255),
            [customer_unique_id]        NVARCHAR(255),
            [customer_zip_code_prefix]  NVARCHAR(255),
            [customer_city]             NVARCHAR(255),
            [customer_state]            NVARCHAR(255)
        );

        -- --------------------------------------------------------
        -- 2. Bulk insert CSV into temp table
        -- --------------------------------------------------------
        SET @sql = '
            BULK INSERT #tmp_customers
            FROM ''' + @file_path + '''
            WITH (
                FIRSTROW = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR = ''0x0a'',
                CODEPAGE = ''65001''
            );';

        EXEC sp_executesql @sql;

        -- --------------------------------------------------------
        -- 3. Append from temp into raw with meta columns
        -- --------------------------------------------------------
        INSERT INTO raw.customers (
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            load_ts,
            file_name
        )
        SELECT
            customer_id,
            customer_unique_id,
            customer_zip_code_prefix,
            customer_city,
            customer_state,
            GETDATE()   AS load_ts,
            @file_name  AS file_name
        FROM #tmp_customers;

        SET @rows = @@ROWCOUNT;
        PRINT 'raw.customers loaded: ' + CAST(@rows AS NVARCHAR) + ' rows';
        PRINT 'File: ' + @file_name;
        PRINT 'Load timestamp: ' + CONVERT(NVARCHAR, GETDATE(), 120);

    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #tmp_customers;
        PRINT 'Error in raw.sp_load_customers: ' + ERROR_MESSAGE();
        THROW;
    END CATCH

    -- --------------------------------------------------------
    -- 4. Cleanup
    -- --------------------------------------------------------
    DROP TABLE IF EXISTS #tmp_customers;

END;
GO
