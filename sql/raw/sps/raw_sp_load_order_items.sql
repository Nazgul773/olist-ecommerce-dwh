USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE raw.sp_load_order_items
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
        DROP TABLE IF EXISTS #tmp_order_items;

        CREATE TABLE #tmp_order_items (
            [order_id]              NVARCHAR(255),
            [order_item_id]         NVARCHAR(255),
            [product_id]            NVARCHAR(255),
            [seller_id]             NVARCHAR(255),
            [shipping_limit_date]   NVARCHAR(255),
            [price]                 NVARCHAR(255),
            [freight_value]         NVARCHAR(255)
        );

        -- --------------------------------------------------------
        -- 2. Bulk insert CSV into temp table
        -- --------------------------------------------------------
        SET @sql = '
            BULK INSERT #tmp_order_items
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
        INSERT INTO raw.order_items (
            batch_id,
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            load_ts,
            file_name
        )
        SELECT
            @batch_id,
            order_id,
            order_item_id,
            product_id,
            seller_id,
            shipping_limit_date,
            price,
            freight_value,
            GETDATE()   AS load_ts,
            @file_name  AS file_name
        FROM #tmp_order_items;

        SET @rows = @@ROWCOUNT;
        PRINT 'raw.order_items loaded: ' + CAST(@rows AS NVARCHAR) + ' rows';
        PRINT 'Batch ID: ' + CAST(@batch_id AS NVARCHAR(36));
        PRINT 'File: ' + @file_name;
        PRINT 'Load timestamp: ' + CONVERT(NVARCHAR, GETDATE(), 120);

    END TRY
    BEGIN CATCH
        DROP TABLE IF EXISTS #tmp_order_items;
        PRINT 'Error in raw.sp_load_order_items: ' + ERROR_MESSAGE();
        THROW;
    END CATCH

    PRINT '--------------------------------------------------------'

    -- --------------------------------------------------------
    -- 4. Cleanup
    -- --------------------------------------------------------
    DROP TABLE IF EXISTS #tmp_order_items;

END;
GO
