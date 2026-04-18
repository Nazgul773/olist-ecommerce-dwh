USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_fact_payments
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
        'MART',      'mart.sp_load_fact_payments', 'mart.fact_payments',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- Full reload: TRUNCATE + INSERT in a transaction. TRUNCATE is rollback-safe in SQL Server.
        -- IDENTITY seed resets each run — acceptable, payment_fact_key is a technical PK only.
        BEGIN TRANSACTION;

        TRUNCATE TABLE mart.fact_payments;

        -- payment_type_key resolved from dim_payment_type by payment_type_name.
        -- An order can have multiple payment records (e.g. voucher + credit_card split).
        -- payment_value per record is fully additive within the same payment_type slice.
        -- To compute total payment per order, SUM across payment_sequential.
        INSERT INTO mart.fact_payments (
            purchase_date_key,
            customer_key,
            payment_type_key,
            order_id,
            payment_sequential,
            payment_installments,
            payment_value
        )
        SELECT
            ISNULL(dd.date_key,         0)  AS purchase_date_key,
            ISNULL(dc.customer_key,    -1)  AS customer_key,
            ISNULL(pt.payment_type_key,-1)  AS payment_type_key,
            p.order_id,
            p.payment_sequential,
            p.payment_installments,
            p.payment_value

        FROM cleansed.order_payments p

        -- Orders: provides purchase date and customer_id
        JOIN cleansed.orders o
            ON  o.order_id   = p.order_id
            AND o.is_deleted = 0

        LEFT JOIN mart.dim_date dd
            ON dd.date_key =   YEAR(o.order_purchase_timestamp)  * 10000
                             + MONTH(o.order_purchase_timestamp) * 100
                             + DAY(o.order_purchase_timestamp)

        LEFT JOIN mart.dim_customer dc
            ON dc.customer_id = o.customer_id

        LEFT JOIN mart.dim_payment_type pt
            ON pt.payment_type_name = p.payment_type

        WHERE p.is_deleted = 0;

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
            @job_run_id, @pipeline_id, 'mart.sp_load_fact_payments',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
