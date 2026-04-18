USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_customer
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
        'MART',      'mart.sp_load_dim_customer', 'mart.dim_customer',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Unknown member: surrogate -1 handles unresolvable FKs in fact tables.
        -- IDENTITY_INSERT required to force the explicit -1 value.
        IF NOT EXISTS (SELECT 1 FROM mart.dim_customer WHERE customer_key = -1)
        BEGIN
            SET IDENTITY_INSERT mart.dim_customer ON;
            INSERT INTO mart.dim_customer (customer_key, customer_id, customer_unique_id, customer_zip_code, customer_city, customer_state, customer_lat, customer_lng)
            VALUES (-1, 'UNKNOWN', 'UNKNOWN', '00000', 'Unknown', 'XX', NULL, NULL);
            SET IDENTITY_INSERT mart.dim_customer OFF;
        END

        -- SCD Type 1 MERGE from cleansed.customers.
        ;WITH src AS (
            SELECT
                c.customer_id,
                c.customer_unique_id,
                c.customer_zip_code_prefix  AS customer_zip_code,
                c.customer_city,
                c.customer_state,
                g.geolocation_lat           AS customer_lat,
                g.geolocation_lng           AS customer_lng,
                HASHBYTES('SHA2_256', CONCAT(
                    c.customer_unique_id,       '|',
                    c.customer_zip_code_prefix, '|',
                    c.customer_city,            '|',
                    c.customer_state,           '|',
                    ISNULL(CAST(g.geolocation_lat AS NVARCHAR(30)), ''), '|',
                    ISNULL(CAST(g.geolocation_lng AS NVARCHAR(30)), '')
                )) AS row_hash
            FROM cleansed.customers c
            LEFT JOIN cleansed.geolocation g
                ON g.geolocation_zip_code_prefix = c.customer_zip_code_prefix
               AND g.is_deleted = 0
            WHERE c.is_deleted = 0
        )
        MERGE mart.dim_customer AS tgt
        USING src
            ON tgt.customer_id = src.customer_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete.
        WHEN MATCHED AND (
            tgt.row_hash <> src.row_hash OR tgt.row_hash IS NULL OR tgt.is_deleted = 1
        ) THEN
            UPDATE SET
                customer_unique_id = src.customer_unique_id,
                customer_zip_code  = src.customer_zip_code,
                customer_city      = src.customer_city,
                customer_state     = src.customer_state,
                customer_lat       = src.customer_lat,
                customer_lng       = src.customer_lng,
                is_deleted         = 0,
                row_hash           = src.row_hash,
                updated_at         = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                customer_id,        customer_unique_id,
                customer_zip_code,  customer_city,
                customer_state,     customer_lat,  customer_lng,
                row_hash
            )
            VALUES (
                src.customer_id,        src.customer_unique_id,
                src.customer_zip_code,  src.customer_city,
                src.customer_state,     src.customer_lat,  src.customer_lng,
                src.row_hash
            )
        -- The unknown member row (customer_key = -1) is excluded from soft deletion.
        WHEN NOT MATCHED BY SOURCE AND tgt.customer_key <> -1 THEN
            UPDATE SET
                is_deleted  = 1,
                updated_at = SYSUTCDATETIME();

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
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_customer',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
