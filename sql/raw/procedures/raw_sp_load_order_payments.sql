USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE raw.sp_load_order_payments
    @file_path   NVARCHAR(500)    = NULL,
    @file_name   NVARCHAR(255)    = NULL,
    @batch_id    UNIQUEIDENTIFIER OUTPUT,
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows        INT           = 0;
    DECLARE @error_msg   NVARCHAR(MAX);
    DECLARE @sql         NVARCHAR(MAX);
    DECLARE @start_time  DATETIME2(3)  = SYSUTCDATETIME();
    DECLARE @duration_ms INT;

    -- Validate required parameters before assigning a batch_id or writing a RUNNING entry,
    -- so a validation failure never produces an orphaned load_log row.
    IF @file_path IS NULL OR @file_name IS NULL
    BEGIN
        SET @error_msg = 'Required parameters missing: file_path or file_name';
        SET @batch_id  = NEWID();

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id,          sp_name,
            error_message, error_ts,    file_name,            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id,         'raw.sp_load_order_payments',
            @error_msg,    SYSUTCDATETIME(), ISNULL(@file_name, 'UNKNOWN'), 16, 'raw.sp_load_order_payments', NULL
        );

        THROW 50001, @error_msg, 1;
    END

    SET @batch_id = NEWID();

    INSERT INTO audit.load_log (
        batch_id,       job_run_id,  pipeline_id,
        layer,          sp_name,     table_name,
        rows_processed, status,      load_ts,          file_name
    )
    VALUES (
        @batch_id,      @job_run_id, @pipeline_id,
        'RAW',          'raw.sp_load_order_payments', 'raw.order_payments',
        0,              'RUNNING',   SYSUTCDATETIME(), @file_name
    );

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Validate file_path before concatenating into dynamic SQL.
        -- Must be a .csv path with no semicolons or single quotes —
        -- semicolons are the primary injection vector after quote-escaping.
        IF @file_path NOT LIKE '%.csv'
            OR @file_path LIKE '%;%'
            OR @file_path LIKE '%''%'
            THROW 50003, 'Invalid file_path: must be a .csv path without semicolons or quotes', 1;

        CREATE TABLE #order_payments_staging (
            order_id               NVARCHAR(255),
            payment_sequential     NVARCHAR(255),
            payment_type           NVARCHAR(255),
            payment_installments   NVARCHAR(255),
            payment_value          NVARCHAR(255)
        );

        SET @sql = '
            BULK INSERT #order_payments_staging
            FROM ''' + REPLACE(@file_path, '''', '''''') + '''
            WITH (
                FIRSTROW        = 2,
                FIELDTERMINATOR = '','',
                ROWTERMINATOR   = ''0x0a'',
                CODEPAGE        = ''65001''
            );';

        EXEC (@sql);

        INSERT INTO raw.order_payments (
            batch_id,    order_id,    payment_sequential,  payment_type,
            payment_installments,     payment_value,        load_ts,   file_name
        )
        SELECT
            @batch_id,   order_id,    payment_sequential,  payment_type,
            payment_installments,     payment_value,        SYSUTCDATETIME(), @file_name
        FROM #order_payments_staging;

        SET @rows        = @@ROWCOUNT;
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @rows,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'raw.sp_load_order_payments';

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
        WHERE batch_id = @batch_id AND sp_name = 'raw.sp_load_order_payments';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,    file_name,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'raw.sp_load_order_payments',
            @error_msg,    SYSUTCDATETIME(), @file_name,
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
