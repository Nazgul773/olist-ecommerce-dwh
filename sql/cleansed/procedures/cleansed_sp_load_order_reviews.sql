USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_order_reviews
    @batch_id    UNIQUEIDENTIFIER OUTPUT,
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @merge_rowcount INT           = 0;
    DECLARE @start_time     DATETIME2(3)  = SYSUTCDATETIME();
    DECLARE @duration_ms    INT;
    DECLARE @error_msg      NVARCHAR(MAX);

    -- Inherit the RAW batch_id so the same ID flows through all layers
    -- (raw → cleansed → mart), enabling end-to-end tracing.
    SELECT @batch_id = src.last_batch_id
    FROM orchestration.pipeline_config cleansed_cfg
    JOIN orchestration.pipeline_config src
        ON src.pipeline_id      = cleansed_cfg.source_pipeline_id
    WHERE cleansed_cfg.pipeline_id = @pipeline_id
      AND src.last_run_status      = 'SUCCESS';

    BEGIN TRY
        INSERT INTO audit.load_log (
            batch_id,       job_run_id,  pipeline_id,
            layer,          sp_name,     table_name,
            rows_processed, status,      load_ts
        )
        VALUES (
            @batch_id,      @job_run_id, @pipeline_id,
            'CLEANSED',     'cleansed.sp_load_order_reviews', 'cleansed.order_reviews',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    ROW_NUMBER() deduplicates on review_id — the Olist dataset contains known
        --    duplicate review_ids; duplicates are logged to dq_log and the first
        --    occurrence (lowest row_id) is retained.
        ;WITH ranked AS (
            SELECT
                row_id,
                review_id,
                order_id,
                review_score,
                review_comment_title,
                review_comment_message,
                review_creation_date,
                review_answer_timestamp,
                REPLACE(TRIM(review_id),               '"', '') AS clean_review_id,
                REPLACE(TRIM(order_id),                '"', '') AS clean_order_id,
                TRY_CAST(REPLACE(TRIM(review_score),   '"', '') AS TINYINT) AS parsed_score,
                REPLACE(TRIM(review_comment_title),    '"', '') AS clean_comment_title,
                REPLACE(TRIM(review_comment_message),  '"', '') AS clean_comment_message,
                TRY_CONVERT(DATETIME2(0), REPLACE(TRIM(review_creation_date),    '"', '')) AS parsed_creation_date,
                TRY_CONVERT(DATETIME2(0), REPLACE(TRIM(review_answer_timestamp), '"', '')) AS parsed_answer_ts,
                ROW_NUMBER() OVER (
                    PARTITION BY REPLACE(TRIM(review_id), '"', '')
                    ORDER BY row_id
                ) AS rn
            FROM raw.order_reviews
            WHERE batch_id = @batch_id
        )
        SELECT
            row_id, review_id, order_id, review_score,
            review_comment_title, review_comment_message,
            review_creation_date, review_answer_timestamp,
            clean_review_id, clean_order_id, parsed_score,
            clean_comment_title, clean_comment_message,
            parsed_creation_date, parsed_answer_ts,
            rn
        INTO #normalized_order_reviews
        FROM ranked;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'review_id'               AS column_name, 'NULL value' AS issue FROM #normalized_order_reviews WHERE clean_review_id IS NULL
            UNION ALL
            SELECT 'order_id',                'NULL value'                         FROM #normalized_order_reviews WHERE clean_order_id IS NULL
            UNION ALL
            SELECT 'review_score',            'NULL value'                         FROM #normalized_order_reviews WHERE review_score IS NULL
            UNION ALL
            SELECT 'review_creation_date',    'NULL value'                         FROM #normalized_order_reviews WHERE review_creation_date IS NULL
            UNION ALL
            SELECT 'review_answer_timestamp', 'NULL value'                         FROM #normalized_order_reviews WHERE review_answer_timestamp IS NULL

            -- Completeness: empty string checks
            UNION ALL
            SELECT 'review_id', 'Empty string after cleansing' FROM #normalized_order_reviews WHERE clean_review_id = ''
            UNION ALL
            SELECT 'order_id',  'Empty string after cleansing' FROM #normalized_order_reviews WHERE clean_order_id = ''

            -- Validity: ID length + hex format
            UNION ALL
            SELECT 'review_id', 'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_reviews WHERE clean_review_id != '' AND (LEN(clean_review_id) != 32 OR clean_review_id LIKE '%[^0-9a-f]%')
            UNION ALL
            SELECT 'order_id',  'Invalid length or format: expected 32-char lowercase hex' FROM #normalized_order_reviews WHERE clean_order_id != ''  AND (LEN(clean_order_id) != 32 OR clean_order_id  LIKE '%[^0-9a-f]%')

            -- Validity: review_score parse failure and range check (must be 1-5)
            UNION ALL
            SELECT 'review_score', 'Invalid numeric format'
            FROM #normalized_order_reviews WHERE review_score IS NOT NULL AND parsed_score IS NULL
            UNION ALL
            SELECT 'review_score', 'Invalid range: must be 1-5'
            FROM #normalized_order_reviews WHERE parsed_score IS NOT NULL AND (parsed_score < 1 OR parsed_score > 5)

            -- Validity: date parse failures
            UNION ALL
            SELECT 'review_creation_date',    'Invalid datetime format' FROM #normalized_order_reviews WHERE review_creation_date    IS NOT NULL AND parsed_creation_date IS NULL
            UNION ALL
            SELECT 'review_answer_timestamp', 'Invalid datetime format' FROM #normalized_order_reviews WHERE review_answer_timestamp IS NOT NULL AND parsed_answer_ts     IS NULL

            -- Duplicates: known dataset characteristic — log, do not abort
            UNION ALL
            SELECT 'review_id', 'Duplicate review_id: kept first occurrence by row_id'
            FROM #normalized_order_reviews
            WHERE rn > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'order_reviews', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash detects changed rows
        --    to avoid unnecessary updates. Rows absent from the current batch are soft-
        --    deleted (is_deleted = 1) rather than removed. Reappearing rows are reactivated.
        --    Only the first occurrence of each review_id (rn = 1) is used as MERGE source.
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_review_id,
                clean_order_id,
                parsed_score,
                clean_comment_title,
                clean_comment_message,
                parsed_creation_date,
                parsed_answer_ts,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_review_id,                                                    '|',
                    clean_order_id,                                                     '|',
                    CAST(parsed_score AS NVARCHAR),                                     '|',
                    ISNULL(clean_comment_title,   ''),                                  '|',
                    ISNULL(clean_comment_message, ''),                                  '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_creation_date, 120), ''),       '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_answer_ts,     120), '')
                )) AS row_hash
            FROM #normalized_order_reviews
            WHERE rn = 1
        )
        MERGE cleansed.order_reviews AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_review_id IS NOT NULL AND clean_review_id != '' AND clean_review_id NOT LIKE '%[^0-9a-f]%'  AND LEN(clean_review_id) = 32
              AND clean_order_id IS NOT NULL AND clean_order_id != '' AND clean_order_id NOT LIKE '%[^0-9a-f]%'   AND LEN(clean_order_id) = 32
              AND parsed_score IS NOT NULL
              AND parsed_creation_date IS NOT NULL
              AND parsed_answer_ts IS NOT NULL
        ) AS src
        ON tgt.review_id = src.clean_review_id
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                order_id                = src.clean_order_id,
                review_score            = src.parsed_score,
                review_comment_title    = NULLIF(src.clean_comment_title,   ''),
                review_comment_message  = NULLIF(src.clean_comment_message, ''),
                review_creation_date    = src.parsed_creation_date,
                review_answer_timestamp = src.parsed_answer_ts,
                row_hash                = src.row_hash,
                is_deleted              = 0,
                deleted_at              = NULL,
                updated_at              = SYSUTCDATETIME()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                review_id,              order_id,
                review_score,           review_comment_title,
                review_comment_message, review_creation_date,
                review_answer_timestamp, row_hash,             updated_at
            )
            VALUES (
                src.clean_review_id,    src.clean_order_id,
                src.parsed_score,       NULLIF(src.clean_comment_title,   ''),
                NULLIF(src.clean_comment_message, ''), src.parsed_creation_date,
                src.parsed_answer_ts,   src.row_hash,                          SYSUTCDATETIME()
            )
        WHEN NOT MATCHED BY SOURCE AND tgt.is_deleted = 0 THEN
            UPDATE SET
                is_deleted = 1,
                deleted_at = SYSUTCDATETIME(),
                updated_at = SYSUTCDATETIME();

        SET @merge_rowcount = @@ROWCOUNT;
        SET @duration_ms    = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @merge_rowcount,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_reviews';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_order_reviews';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_order_reviews',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
