USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE raw.sp_load_orders
    @file_path  NVARCHAR(500),
    @file_name  NVARCHAR(255),
    @batch_id   UNIQUEIDENTIFIER OUTPUT
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @sql  NVARCHAR(MAX);
    DECLARE @rows INT;

    SET @batch_id = NEWID();

    BEGIN TRY
        -- --------------------------------------------------------
        -- 1. Create temp table
        -- --------------------------------------------------------
        DROP TABLE IF EXISTS #tmp_orders;

        CREATE TABLE #tmp_orders (
            [order_id]                          NVARCHAR(255),
            [customer_id]                       NVARCHAR(255),
            [order_status]                      NVARCHAR(255),
            [order_purchase_timestamp]          NVARCHAR(255),
            [order_approved_at]                 NVARCHAR(255),
            [order_delivered_carrier_date]      NVARCHAR(255),
            [order_delivered_customer_date]     NVARCHAR(255),
            [order_estimated_delivery_date]     NVARCHAR(255)
        );

        -- --------------------------------------------------------
        -- 2. Bulk insert CSV into temp table
        -- --------------------------------------------------------
        SET @sql = '
            BULK INSERT #tmp_orders
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
        INSERT INTO raw.orders (
            batch_id,
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            load_ts,
            file_name
        )
        SELECT
            @batch_id,
            order_id,
            customer_id,
            order_status,
            order_purchase_timestamp,
            order_approved_at,
            order_delivered_carrier_date,
            order_delivered_customer_date,
            order_estimated_delivery_date,
            GETDATE()   AS load_ts,
            @file_name  AS file_name
        FROM #tmp_orders;

        SET @rows = @@ROWCOUNT;
        PRINT 'raw.orders loaded: ' + CAST(@rows AS NVARCHAR) + ' rows';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR(36));
        PRINT 'File: ' + @file_name;
        PRINT 'Load timestamp: ' + CONVERT(NVARCHAR, GETDATE(), 120);

    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #tmp_orders;
        PRINT 'Error in raw.sp_load_orders: ' + ERROR_MESSAGE();
        THROW;
    END CATCH

    PRINT '--------------------------------------------------------'

    -- --------------------------------------------------------
    -- 4. Cleanup
    -- --------------------------------------------------------
    DROP TABLE IF EXISTS #tmp_orders;

END;
GO
