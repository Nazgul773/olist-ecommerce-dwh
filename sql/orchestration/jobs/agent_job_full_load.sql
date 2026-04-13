USE msdb;
GO

-- ============================================================
-- Job:     OlistDWH_Orchestration_FullLoad_Daily
-- Steps:
--   1. CmdExec  — Preprocess CSVs
--   2. T-SQL    — Execute Full Load Pipeline
--
-- Step flow:
--   Step 1 success -> Step 2 | Step 1 fail -> quit failure
--   Step 2 success -> quit success | Step 2 fail -> quit failure
--
-- Note on preprocessed pipe files:
--   preprocess_all.ps1 overwrites the output files on every run,
--   so no cleanup step is needed — files do not accumulate.
--   A failed load leaves the pipe file on disk for debugging.
--
-- SETUP (required before running):
--   Update @ScriptRoot below to match the environment.
--   DatasetRoot is derived dynamically from orchestration.pipeline_config.
-- ============================================================

DECLARE @ScriptRoot  NVARCHAR(500) = 'D:\Code\VCS Projects\olist-ecommerce-dwh\scripts\ps';

-- Create job if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysjobs WHERE name = N'OlistDWH_Orchestration_FullLoad_Daily')
    EXEC dbo.sp_add_job
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily';

-- Create schedule if it doesn't exist
IF NOT EXISTS (SELECT 1 FROM msdb.dbo.sysschedules WHERE name = N'Daily_0200')
    EXEC dbo.sp_add_schedule
        @schedule_name     = N'Daily_0200',
        @freq_type         = 4,
        @freq_interval     = 1,
        @active_start_time = 020000;

DECLARE @job_id UNIQUEIDENTIFIER;
SELECT @job_id = job_id FROM msdb.dbo.sysjobs WHERE name = N'OlistDWH_Orchestration_FullLoad_Daily';

-- Remove all existing steps so they can be recreated in the correct order.
-- sp_delete_jobstep renumbers remaining steps downward, so deleting step_id = 1
-- repeatedly is the correct way to drain the list.
WHILE EXISTS (SELECT 1 FROM msdb.dbo.sysjobsteps WHERE job_id = @job_id)
    EXEC dbo.sp_delete_jobstep @job_id = @job_id, @step_id = 1;

-- ---------------------------------------------------------------------------
-- Step 1 — Preprocess CSVs
-- Calls preprocess_all.ps1, which converts all active RAW pipelines flagged with
-- needs_preprocessing = 1 from comma-delimited (with quoted fields) to pipe-
-- delimited (no quoting), required by BULK INSERT on SQL Server on-premises
-- (IID_IColumnsInfo OLE DB limitation).
-- To override the SQL Server instance (default: localhost), append
-- -SqlInstance <instance> to the command below.
-- ---------------------------------------------------------------------------
DECLARE @preprocess_cmd NVARCHAR(MAX) =
    'powershell.exe -ExecutionPolicy Bypass -File "' + @ScriptRoot + '\preprocess_all.ps1"';

EXEC dbo.sp_add_jobstep
    @job_name          = N'OlistDWH_Orchestration_FullLoad_Daily',
    @step_name         = N'Preprocess CSVs',
    @step_id           = 1,
    @subsystem         = N'CmdExec',
    @command           = @preprocess_cmd,
    @on_success_action = 3,   -- go to next step
    @on_fail_action    = 2;   -- quit with failure

-- ---------------------------------------------------------------------------
-- Step 2 — Execute Full Load Pipeline
-- Runs all RAW -> CLEANSED -> MART layers via the orchestration SPs (sp_run_full_load -> sp_run_layer).
-- ---------------------------------------------------------------------------
EXEC dbo.sp_add_jobstep
    @job_name          = N'OlistDWH_Orchestration_FullLoad_Daily',
    @step_name         = N'Execute Full Load Pipeline',
    @step_id           = 2,
    @subsystem         = N'TSQL',
    @database_name     = N'OlistDWH',
    @command           = N'EXEC orchestration.sp_run_full_load @triggered_by = ''AGENT_JOB'';',
    @on_success_action = 1,   -- quit with success
    @on_fail_action    = 2;   -- quit with failure

-- Attach schedule to job if not already attached
DECLARE @schedule_id INT;
SELECT @schedule_id = schedule_id FROM msdb.dbo.sysschedules WHERE name = N'Daily_0200';

IF NOT EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobschedules js
    JOIN msdb.dbo.sysjobs         j ON js.job_id      = j.job_id
    JOIN msdb.dbo.sysschedules    s ON js.schedule_id = s.schedule_id
    WHERE j.name = N'OlistDWH_Orchestration_FullLoad_Daily'
      AND s.name = N'Daily_0200'
)
    EXEC dbo.sp_attach_schedule
        @job_name      = N'OlistDWH_Orchestration_FullLoad_Daily',
        @schedule_name = N'Daily_0200';

-- Register job on this server if not already registered
IF NOT EXISTS (
    SELECT 1
    FROM msdb.dbo.sysjobservers js
    JOIN msdb.dbo.sysjobs       j ON js.job_id = j.job_id
    WHERE j.name = N'OlistDWH_Orchestration_FullLoad_Daily'
)
    EXEC dbo.sp_add_jobserver
        @job_name = N'OlistDWH_Orchestration_FullLoad_Daily';
GO
