USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE orchestration.sp_run_layer
    @layer      NVARCHAR(20),
    @job_run_id UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @pipeline_id      INT;
    DECLARE @sp_name          NVARCHAR(255);
    DECLARE @file_path        NVARCHAR(500);
    DECLARE @file_name        NVARCHAR(255);
    DECLARE @batch_id         UNIQUEIDENTIFIER;
    DECLARE @sql              NVARCHAR(MAX);
    DECLARE @error_msg        NVARCHAR(MAX);
    DECLARE @layer_failed     BIT          = 0;
    DECLARE @first_error      NVARCHAR(MAX);
    DECLARE @child_sp_invoked BIT          = 0;

    DECLARE etl_cursor CURSOR FOR
        SELECT pipeline_id, sp_name, file_path, file_name
        FROM orchestration.pipeline_config
        WHERE layer     = @layer
          AND is_active = 1
        ORDER BY load_sequence ASC;

    OPEN etl_cursor;
    FETCH NEXT FROM etl_cursor INTO @pipeline_id, @sp_name, @file_path, @file_name;

    WHILE @@FETCH_STATUS = 0
    BEGIN
        -- Stop on first failure — later pipelines may depend on earlier ones via load_sequence.
        IF @layer_failed = 1
            BREAK;

        BEGIN TRY
            SET @child_sp_invoked = 0;

            -- Validate sp_name against the catalog before executing.
            -- Prevents injection via pipeline_config and catches misconfigured entries.
            IF NOT EXISTS (
                SELECT 1
                FROM sys.procedures p
                JOIN sys.schemas    s ON s.schema_id = p.schema_id
                WHERE s.name + '.' + p.name = @sp_name
            )
                THROW 50002, 'sp_name in pipeline_config does not resolve to an existing procedure', 1;

            IF @layer = 'RAW'
            BEGIN
                SET @sql = 'EXEC '
                    + QUOTENAME(PARSENAME(@sp_name, 2)) + '.' + QUOTENAME(PARSENAME(@sp_name, 1))
                    + ' @file_path   = @fp,'
                    + ' @file_name   = @fn,'
                    + ' @batch_id    = @bid OUTPUT,'
                    + ' @pipeline_id = @pid,'
                    + ' @job_run_id  = @jid';

                SET @child_sp_invoked = 1;
                EXEC sp_executesql @sql,
                    N'@fp NVARCHAR(500), @fn NVARCHAR(255), @bid UNIQUEIDENTIFIER OUTPUT, @pid INT, @jid UNIQUEIDENTIFIER',
                    @fp  = @file_path,
                    @fn  = @file_name,
                    @bid = @batch_id OUTPUT,
                    @pid = @pipeline_id,
                    @jid = @job_run_id;
            END
            ELSE IF @layer = 'CLEANSED'
            BEGIN
                -- CLEANSED: inherits RAW batch_id via source_pipeline_id
                SET @sql = 'EXEC '
                    + QUOTENAME(PARSENAME(@sp_name, 2)) + '.' + QUOTENAME(PARSENAME(@sp_name, 1))
                    + ' @batch_id    = @bid OUTPUT,'
                    + ' @pipeline_id = @pid,'
                    + ' @job_run_id  = @jid';

                SET @child_sp_invoked = 1;
                EXEC sp_executesql @sql,
                    N'@bid UNIQUEIDENTIFIER OUTPUT, @pid INT, @jid UNIQUEIDENTIFIER',
                    @bid = @batch_id OUTPUT,
                    @pid = @pipeline_id,
                    @jid = @job_run_id;
            END
            ELSE -- MART: no batch_id; cross-layer tracing via job_run_id
            BEGIN
                SET @sql = 'EXEC '
                    + QUOTENAME(PARSENAME(@sp_name, 2)) + '.' + QUOTENAME(PARSENAME(@sp_name, 1))
                    + ' @pipeline_id = @pid,'
                    + ' @job_run_id  = @jid';

                SET @child_sp_invoked = 1;
                EXEC sp_executesql @sql,
                    N'@pid INT, @jid UNIQUEIDENTIFIER',
                    @pid = @pipeline_id,
                    @jid = @job_run_id;
            END

            UPDATE orchestration.pipeline_config
            SET last_run_ts     = SYSUTCDATETIME(),
                last_run_status = 'SUCCESS',
                last_batch_id   = @batch_id
            WHERE pipeline_id   = @pipeline_id;

        END TRY
        BEGIN CATCH
            SET @error_msg = ERROR_MESSAGE();

            SET @layer_failed = 1;
            IF @first_error IS NULL
                SET @first_error = @error_msg;

            UPDATE orchestration.pipeline_config
            SET last_run_ts     = SYSUTCDATETIME(),
                last_run_status = 'FAILED'
            WHERE pipeline_id   = @pipeline_id;

            -- Only log here if the error originated in orchestration code (before any child SP
            -- was invoked). Child SPs log their own errors before re-throwing; logging again
            -- here would create duplicates.
            IF @child_sp_invoked = 0
            BEGIN
                INSERT INTO audit.error_log (
                    batch_id,      job_run_id,  pipeline_id, sp_name,
                    error_message, error_ts,    file_name,
                    error_severity, error_procedure, error_line
                )
                VALUES (
                    @batch_id,     @job_run_id, @pipeline_id, @sp_name,
                    @error_msg,    SYSUTCDATETIME(), @file_name,
                    ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
                );
            END
        END CATCH

        FETCH NEXT FROM etl_cursor INTO @pipeline_id, @sp_name, @file_path, @file_name;
    END

    CLOSE etl_cursor;
    DEALLOCATE etl_cursor;

    -- Propagate layer failure so the caller and the Agent Job step both see a hard error.
    IF @layer_failed = 1
        THROW 50101, @first_error, 1;
END;
GO
