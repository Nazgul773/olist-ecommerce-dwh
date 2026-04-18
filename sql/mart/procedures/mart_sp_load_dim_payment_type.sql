USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_payment_type
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows        INT       = 0;
    DECLARE @start_time  DATETIME2 = SYSUTCDATETIME();
    DECLARE @duration_ms INT;
    DECLARE @error_msg   NVARCHAR(MAX);
    DECLARE @log_id      INT;

    INSERT INTO audit.load_log (
        job_run_id,  pipeline_id,
        layer,       sp_name,     table_name,
        rows_processed, status,  load_ts
    )
    VALUES (
        @job_run_id, @pipeline_id,
        'MART',      'mart.sp_load_dim_payment_type', 'mart.dim_payment_type',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Idempotent seed of all payment types.
        -- dim_payment_type has no IDENTITY column — INT PK allows explicit values.
        -- Key -1 = unknown member (fallback for unresolvable FK in fact_payments).
        -- Keys 1–5 match the five distinct values from the dataset.
        INSERT INTO mart.dim_payment_type (payment_type_key, payment_type_name)
        SELECT v.payment_type_key, v.payment_type_name
        FROM (VALUES
            (-1, 'Unknown'),
            ( 1, 'credit_card'),
            ( 2, 'boleto'),
            ( 3, 'voucher'),
            ( 4, 'debit_card'),
            ( 5, 'not_defined')
        ) AS v(payment_type_key, payment_type_name)
        WHERE NOT EXISTS (
            SELECT 1 FROM mart.dim_payment_type
            WHERE payment_type_key = v.payment_type_key
        );

        SET @rows        = @@ROWCOUNT;
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @rows,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE log_id = @log_id;

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
        WHERE log_id = @log_id;

        INSERT INTO audit.error_log (
            job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_payment_type',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
