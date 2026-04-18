USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_product
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
        'MART',      'mart.sp_load_dim_product', 'mart.dim_product',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Unknown member: surrogate -1 handles unresolvable FKs in fact tables.
        -- IDENTITY_INSERT required to force the explicit -1 value.
        IF NOT EXISTS (SELECT 1 FROM mart.dim_product WHERE product_key = -1)
        BEGIN
            SET IDENTITY_INSERT mart.dim_product ON;
            INSERT INTO mart.dim_product (product_key, product_id, product_category_name, product_category_name_english,
                product_name_length, product_description_length, product_photos_qty,
                product_weight_g, product_length_cm, product_height_cm, product_width_cm)
            VALUES (-1, 'UNKNOWN', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL);
            SET IDENTITY_INSERT mart.dim_product OFF;
        END

        -- SCD Type 1 MERGE from cleansed.products.
        ;WITH src AS (
            SELECT
                p.product_id,
                p.product_category_name,
                t.product_category_name_english,
                p.product_name_lenght        AS product_name_length,
                p.product_description_lenght AS product_description_length,
                p.product_photos_qty,
                p.product_weight_g,
                p.product_length_cm,
                p.product_height_cm,
                p.product_width_cm,
                HASHBYTES('SHA2_256', CONCAT(
                    ISNULL(p.product_category_name,         ''), '|',
                    ISNULL(t.product_category_name_english, ''), '|',
                    ISNULL(CAST(p.product_name_lenght        AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_description_lenght AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_photos_qty         AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_weight_g           AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_length_cm          AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_height_cm          AS NVARCHAR(10)), ''), '|',
                    ISNULL(CAST(p.product_width_cm           AS NVARCHAR(10)), '')
                )) AS row_hash
            FROM cleansed.products p
            LEFT JOIN cleansed.product_category_name_translation t
                ON t.product_category_name = p.product_category_name
               AND t.is_deleted = 0
            WHERE p.is_deleted = 0
        )
        MERGE mart.dim_product AS tgt
        USING src
            ON tgt.product_id = src.product_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete.
        WHEN MATCHED AND (
            tgt.row_hash <> src.row_hash OR tgt.row_hash IS NULL OR tgt.is_deleted = 1
        ) THEN
            UPDATE SET
                product_category_name         = src.product_category_name,
                product_category_name_english = src.product_category_name_english,
                product_name_length           = src.product_name_length,
                product_description_length    = src.product_description_length,
                product_photos_qty            = src.product_photos_qty,
                product_weight_g              = src.product_weight_g,
                product_length_cm             = src.product_length_cm,
                product_height_cm             = src.product_height_cm,
                product_width_cm              = src.product_width_cm,
                is_deleted                    = 0,
                row_hash                      = src.row_hash,
                updated_at                    = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                product_id,                    product_category_name,
                product_category_name_english, product_name_length,
                product_description_length,    product_photos_qty,
                product_weight_g,              product_length_cm,
                product_height_cm,             product_width_cm,
                row_hash
            )
            VALUES (
                src.product_id,                    src.product_category_name,
                src.product_category_name_english, src.product_name_length,
                src.product_description_length,    src.product_photos_qty,
                src.product_weight_g,              src.product_length_cm,
                src.product_height_cm,             src.product_width_cm,
                src.row_hash
            )
        -- The unknown member row (product_key = -1) is excluded from soft deletion.
        WHEN NOT MATCHED BY SOURCE AND tgt.product_key <> -1 THEN
            UPDATE SET
                is_deleted = 1,
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
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_product',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
