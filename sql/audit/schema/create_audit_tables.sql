USE OlistDWH;
GO

-- Job Log table (referenced by FK in other tables)
CREATE TABLE audit.job_log (
    job_log_id       INT IDENTITY(1,1)  NOT NULL,
    job_run_id       UNIQUEIDENTIFIER   NOT NULL
                         CONSTRAINT uq_job_log_job_run_id UNIQUE,
    job_name         NVARCHAR(255)      NOT NULL,
    start_ts         DATETIME2(3)       NOT NULL,
    end_ts           DATETIME2(3)       NULL,
    duration_ms      INT                NULL,
    status           NVARCHAR(20)       NOT NULL
                         CONSTRAINT chk_job_log_status
                         CHECK (status IN ('RUNNING', 'SUCCESS', 'PARTIAL', 'FAILED')),
    pipeline_count   INT                NULL,
    pipelines_failed INT                NULL,
    triggered_by     NVARCHAR(100)      NULL,
    CONSTRAINT PK_job_log PRIMARY KEY (job_log_id)
);
GO

CREATE TABLE audit.load_log (
    log_id                INT IDENTITY(1,1)  NOT NULL,
    batch_id              UNIQUEIDENTIFIER   NULL,
    job_run_id            UNIQUEIDENTIFIER   NULL
                              CONSTRAINT fk_load_log_job
                              REFERENCES audit.job_log(job_run_id),
    pipeline_id           INT                NULL
                              CONSTRAINT fk_load_log_pipeline
                              FOREIGN KEY REFERENCES orchestration.pipeline_config(pipeline_id),
    layer                 NVARCHAR(20)       NOT NULL
                              CONSTRAINT chk_load_log_layer
                              CHECK (layer IN ('RAW', 'CLEANSED', 'MART')),
    sp_name               NVARCHAR(255)      NOT NULL,
    table_name            NVARCHAR(100)      NOT NULL,
    rows_processed        INT                NOT NULL,
    status                NVARCHAR(20)       NOT NULL
                              CONSTRAINT chk_load_log_status
                              CHECK (status IN ('RUNNING', 'SUCCESS', 'FAILED')),
    load_ts               DATETIME2(3)       NOT NULL DEFAULT SYSUTCDATETIME(),
    file_name             NVARCHAR(255)      NULL,
    processed_duration_ms INT                NULL,
    CONSTRAINT PK_load_log PRIMARY KEY (log_id)
);
GO

CREATE TABLE audit.error_log (
    error_id         INT IDENTITY(1,1)  NOT NULL,
    batch_id         UNIQUEIDENTIFIER   NULL,
    job_run_id       UNIQUEIDENTIFIER   NULL
                         CONSTRAINT fk_error_log_job
                         REFERENCES audit.job_log(job_run_id),
    pipeline_id      INT                NULL
                         CONSTRAINT fk_error_log_pipeline
                         FOREIGN KEY REFERENCES orchestration.pipeline_config(pipeline_id),
    sp_name          NVARCHAR(255)      NOT NULL,
    error_message    NVARCHAR(MAX)      NOT NULL,
    error_ts         DATETIME2(3)       NOT NULL DEFAULT SYSUTCDATETIME(),
    file_name        NVARCHAR(255)      NULL,
    error_severity   INT                NULL,
    error_procedure  NVARCHAR(128)      NULL,
    error_line       INT                NULL,
    CONSTRAINT PK_error_log PRIMARY KEY (error_id)
);
GO

CREATE TABLE audit.dq_log (
    dq_log_id          INT IDENTITY(1,1)   NOT NULL,
    batch_id           UNIQUEIDENTIFIER    NOT NULL,
    job_run_id         UNIQUEIDENTIFIER    NULL
                           CONSTRAINT fk_dq_log_job
                           REFERENCES audit.job_log(job_run_id),
    table_name         NVARCHAR(100)       NOT NULL,
    column_name        NVARCHAR(100)       NOT NULL,
    issue              NVARCHAR(255)       NOT NULL,
    affected_row_count INT                 NOT NULL,
    log_ts             DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_dq_log PRIMARY KEY (dq_log_id)
);
GO
