USE OlistDWH;
GO

-- Trigger: auto-update modified_ts on any UPDATE to pipeline_config
CREATE OR ALTER TRIGGER orchestration.trg_pipeline_config_modified
ON orchestration.pipeline_config
AFTER UPDATE
AS
BEGIN
    SET NOCOUNT ON;

    UPDATE orchestration.pipeline_config
    SET    modified_ts = SYSUTCDATETIME()
    FROM   orchestration.pipeline_config pc
    INNER JOIN inserted i ON pc.pipeline_id = i.pipeline_id;
END;
GO
