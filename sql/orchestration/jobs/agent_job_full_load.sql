USE msdb;
GO

-- Create job if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'OlistDWH_Orchestration_FullLoad_Daily')
BEGIN
    EXEC dbo.sp_add_job
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily';
END

-- Add schedule if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'Daily_0200')
BEGIN
    EXEC dbo.sp_add_schedule
        @schedule_name = N'Daily_0200',
        @freq_type = 4,  -- Daily
        @freq_interval = 1,
        @active_start_time = 020000;
END

-- Update or create job step
DECLARE @job_id uniqueidentifier;
SELECT @job_id = job_id FROM msdb.dbo.sysjobs WHERE name = N'OlistDWH_Orchestration_FullLoad_Daily';

IF EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps WHERE job_id = @job_id AND step_name = N'Execute Full Load Pipeline')
BEGIN
    -- Get existing step_id for update
    DECLARE @step_id int;
    SELECT @step_id = step_id
    FROM msdb.dbo.sysjobsteps
    WHERE job_id = @job_id
    AND step_name = N'Execute Full Load Pipeline';

    -- Update existing job step with step_id
    EXEC dbo.sp_update_jobstep
        @job_id = @job_id,
        @step_id = @step_id,
        @subsystem = N'TSQL',
        @database_name = N'OlistDWH',
        @command = N'EXEC orchestration.sp_run_full_load @triggered_by = ''AGENT_JOB'';';
END
ELSE
BEGIN
    -- Create new job step
    EXEC dbo.sp_add_jobstep
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily',
        @step_name = N'Execute Full Load Pipeline',
        @subsystem = N'TSQL',
        @database_name = N'OlistDWH',
        @command = N'EXEC orchestration.sp_run_full_load @triggered_by = ''AGENT_JOB'';';
END

-- Attach schedule to job if not already attached
DECLARE @schedule_id int;
SELECT @schedule_id = schedule_id FROM msdb.dbo.sysschedules WHERE name = N'Daily_0200';

IF NOT EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobschedules js
    INNER JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
    INNER JOIN msdb.dbo.sysschedules s ON js.schedule_id = s.schedule_id
    WHERE j.name = N'OlistDWH_Orchestration_FullLoad_Daily'
    AND s.name = N'Daily_0200'
)
BEGIN
    EXEC dbo.sp_attach_schedule
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily',
        @schedule_name = N'Daily_0200';
END

-- Add job to SQL Server Agent if not already added
IF NOT EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobservers js
    INNER JOIN msdb.dbo.sysjobs j ON js.job_id = j.job_id
    WHERE j.name = N'OlistDWH_Orchestration_FullLoad_Daily'
)
BEGIN
    EXEC dbo.sp_add_jobserver
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily';
END
GO
