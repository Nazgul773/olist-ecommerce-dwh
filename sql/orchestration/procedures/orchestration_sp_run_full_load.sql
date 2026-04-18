USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE orchestration.sp_run_full_load
    @triggered_by NVARCHAR(100) = 'MANUAL'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @job_run_id       UNIQUEIDENTIFIER = NEWID();
    DECLARE @start_ts         DATETIME2        = SYSUTCDATETIME();
    DECLARE @pipelines_failed INT              = 0;
    DECLARE @pipelines_total  INT              = 0;

    INSERT INTO audit.job_log (job_run_id, job_name, start_ts, status, triggered_by)
    VALUES (@job_run_id, 'FULL_LOAD', @start_ts, 'RUNNING', @triggered_by);

    -- Execute layers in order; sp_run_layer throws on failure, stopping subsequent layers.
    BEGIN TRY
        EXEC orchestration.sp_run_layer 'RAW',      @job_run_id;
        EXEC orchestration.sp_run_layer 'CLEANSED', @job_run_id;
        EXEC orchestration.sp_run_layer 'MART',     @job_run_id;
    END TRY
    BEGIN CATCH
        SELECT
            @pipelines_failed = COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN pipeline_id END),
            @pipelines_total  = COUNT(DISTINCT pipeline_id)
        FROM audit.load_log
        WHERE job_run_id = @job_run_id
          AND status IN ('SUCCESS', 'FAILED');

        UPDATE audit.job_log
        SET end_ts           = SYSUTCDATETIME(),
            duration_ms      = DATEDIFF(MILLISECOND, @start_ts, SYSUTCDATETIME()),
            status           = CASE
                                   WHEN @pipelines_failed = @pipelines_total THEN 'FAILED'
                                   ELSE 'PARTIAL'
                               END,
            pipeline_count   = @pipelines_total,
            pipelines_failed = @pipelines_failed
        WHERE job_run_id = @job_run_id;

        THROW;
    END CATCH

    -- Reached only when all layers succeeded.
    SELECT
        @pipelines_failed = COUNT(DISTINCT CASE WHEN status = 'FAILED' THEN pipeline_id END),
        @pipelines_total  = COUNT(DISTINCT pipeline_id)
    FROM audit.load_log
    WHERE job_run_id = @job_run_id
      AND status IN ('SUCCESS', 'FAILED');

    UPDATE audit.job_log
    SET end_ts           = SYSUTCDATETIME(),
        duration_ms      = DATEDIFF(MILLISECOND, @start_ts, SYSUTCDATETIME()),
        status           = 'SUCCESS',
        pipeline_count   = @pipelines_total,
        pipelines_failed = @pipelines_failed
    WHERE job_run_id = @job_run_id;
END;
GO
