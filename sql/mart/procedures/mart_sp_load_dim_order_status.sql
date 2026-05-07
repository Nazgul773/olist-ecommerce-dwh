USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_order_status
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
        'MART',      'mart.sp_load_dim_order_status', 'mart.dim_order_status',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Idempotent seed of all order status values.
        -- dim_order_status has no IDENTITY column — INT PK allows explicit values.
        -- Key -1 = unknown member (fallback for unresolvable FK in fact_sales).
        -- Keys 1–8 match the eight distinct values from the dataset.
        -- sort_order reflects the natural order lifecycle (created -> ... -> delivered).
        -- status_category groups statuses for funnel analysis
        INSERT INTO mart.dim_order_status (order_status_key, status_name, status_category, sort_order)
        SELECT v.order_status_key, v.status_name, v.status_category, v.sort_order
        FROM (VALUES
            (-1, 'Unknown',     'Unknown',      0),
            ( 1, 'Created',     'In Progress',  1),
            ( 2, 'Approved',    'In Progress',  2),
            ( 3, 'Invoiced',    'In Progress',  3),
            ( 4, 'Processing',  'In Progress',  4),
            ( 5, 'Shipped',     'In Progress',  5),
            ( 6, 'Delivered',   'Completed',    6),
            ( 7, 'Canceled',    'Canceled',     7),
            ( 8, 'Unavailable', 'Canceled',     8)
        ) AS v(order_status_key, status_name, status_category, sort_order)
        WHERE NOT EXISTS (
            SELECT 1 FROM mart.dim_order_status
            WHERE order_status_key = v.order_status_key
        );

        SET @rows        = @@ROWCOUNT;
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @rows,
            rows_inserted         = @rows,
            rows_updated          = 0,
            rows_deleted          = 0,
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
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_order_status',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
