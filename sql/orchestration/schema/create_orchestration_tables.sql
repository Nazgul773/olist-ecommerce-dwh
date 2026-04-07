USE OlistDWH;
GO
-- DDL: orchestration schema tables

-- ETL control table
CREATE TABLE orchestration.pipeline_config (
    pipeline_id        INT IDENTITY(1,1)  PRIMARY KEY,
    layer              NVARCHAR(20)     NOT NULL,
    table_name         NVARCHAR(255)    NOT NULL,
    sp_name            NVARCHAR(255)    NOT NULL,
    file_path          NVARCHAR(500)    NULL,
    file_name          NVARCHAR(255)    NULL,
    load_sequence      INT              NOT NULL,
    is_active          BIT              NOT NULL DEFAULT 1,
    last_run_ts        DATETIME2(3)     NULL,
    last_run_status    NVARCHAR(20)     NULL,
    last_batch_id      UNIQUEIDENTIFIER NULL,
    -- Points to the upstream pipeline whose last_batch_id this pipeline reads.
    source_pipeline_id INT              NULL,
    created_ts         DATETIME2(3)     NOT NULL DEFAULT SYSUTCDATETIME(),
    modified_ts        DATETIME2(3)     NULL,
    CONSTRAINT FK_pipeline_config_source FOREIGN KEY (source_pipeline_id)
        REFERENCES orchestration.pipeline_config (pipeline_id)
);
GO
