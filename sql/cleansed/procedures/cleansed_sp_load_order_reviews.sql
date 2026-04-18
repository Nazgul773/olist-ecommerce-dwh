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

    -- Inherit the RAW batch_id so the same ID flows through RAW and CLEANSED,
    -- enabling layer-to-layer tracing via batch_id. Cross-layer (mart) tracing
    -- uses job_run_id, which flows through all three layers.
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
        --    Three-stage CTE:
        --      'normalized' — computes clean values once from raw.
        --      'hashed'     — computes row_hash once; reused in both DQ checks and MERGE.
        --      'ranked'     — applies ROW_NUMBER() for deduplication on review_id.
        --    Duplicate handling distinguishes two types logged separately to dq_log:
        --      Type A — same review_id, identical content (hash match): load artefact,
        --               deduplicated silently.
        --      Type B — same review_id, conflicting content (hash mismatch): data quality
        --               conflict, aborts — investigate dq_log before reloading.
        --    ISNULL sentinels in ORDER BY push rows with NULL parsed dates behind valid
        --    rows, ensuring unparseable records are never selected as canonical.
        ;WITH normalized AS (
            SELECT
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
                TRY_CONVERT(DATETIME2(0), REPLACE(TRIM(review_answer_timestamp), '"', '')) AS parsed_answer_ts
            FROM raw.order_reviews
            WHERE batch_id = @batch_id
        ),
        hashed AS (
            SELECT
                *,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_review_id,                                                        '|',
                    ISNULL(CAST(parsed_score AS NVARCHAR),                            ''),  '|',
                    ISNULL(clean_comment_title,                                       ''),  '|',
                    ISNULL(clean_comment_message,                                     ''),  '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_creation_date, 120),          ''),  '|',
                    ISNULL(CONVERT(NVARCHAR(19), parsed_answer_ts,     120),          '')
                )) AS row_hash
            FROM normalized
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_review_id
                    ORDER BY
                        ISNULL(parsed_creation_date, '9999-12-31'),
                        ISNULL(parsed_answer_ts,     '9999-12-31'),
                        clean_order_id
                ) AS rn
            FROM hashed
        )
        SELECT
            review_id, order_id, review_score,
            review_comment_title, review_comment_message,
            review_creation_date, review_answer_timestamp,
            clean_review_id, clean_order_id, parsed_score,
            clean_comment_title, clean_comment_message,
            parsed_creation_date, parsed_answer_ts,
            row_hash, rn
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

            -- Duplicates Type A: same review_id, identical content (hash match) — load artefact
            UNION ALL
            SELECT 'review_id', 'Duplicate review_id: identical content, deduplicated silently'
            FROM #normalized_order_reviews n
            WHERE rn > 1
              AND EXISTS (
                  SELECT 1 FROM #normalized_order_reviews canon
                  WHERE canon.clean_review_id = n.clean_review_id
                    AND canon.rn              = 1
                    AND canon.row_hash        = n.row_hash
              )

            -- Duplicates Type B: same review_id, conflicting content (hash mismatch) — data quality conflict
            UNION ALL
            SELECT 'review_id', 'Duplicate review_id: conflicting content — investigate before reload'
            FROM #normalized_order_reviews n
            WHERE rn > 1
              AND NOT EXISTS (
                  SELECT 1 FROM #normalized_order_reviews canon
                  WHERE canon.clean_review_id = n.clean_review_id
                    AND canon.rn              = 1
                    AND canon.row_hash        = n.row_hash
              )

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'order_reviews', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- Abort on Type B duplicates: conflicting content under the same review_id cannot be
        -- resolved deterministically.
        IF EXISTS (
            SELECT 1 FROM audit.dq_log
            WHERE batch_id    = @batch_id
              AND table_name  = 'order_reviews'
              AND column_name = 'review_id'
              AND issue LIKE 'Duplicate review_id: conflicting content%'
        )
            THROW 50005, 'Conflicting duplicate review_id values detected in batch. Investigate dq_log before reloading.', 1;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    Only rn = 1 rows (canonical per review_id) enter the MERGE as source.
        BEGIN TRANSACTION;
        MERGE cleansed.order_reviews AS tgt
        USING (
            SELECT *
            FROM #normalized_order_reviews
            WHERE rn = 1
              AND clean_review_id IS NOT NULL AND clean_review_id != '' AND clean_review_id NOT LIKE '%[^0-9a-f]%'  AND LEN(clean_review_id) = 32
              AND clean_order_id  IS NOT NULL AND clean_order_id  != '' AND clean_order_id  NOT LIKE '%[^0-9a-f]%'  AND LEN(clean_order_id)  = 32
              AND parsed_score         IS NOT NULL
              AND parsed_creation_date IS NOT NULL
              AND parsed_answer_ts     IS NOT NULL
        ) AS src
        ON tgt.review_id = src.clean_review_id
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
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
                review_id,               order_id,
                review_score,            review_comment_title,
                review_comment_message,  review_creation_date,
                review_answer_timestamp, row_hash,             updated_at
            )
            VALUES (
                src.clean_review_id,     src.clean_order_id,
                src.parsed_score,        NULLIF(src.clean_comment_title,   ''),
                NULLIF(src.clean_comment_message, ''), src.parsed_creation_date,
                src.parsed_answer_ts,    src.row_hash,                          SYSUTCDATETIME()
            )
        -- Soft delete: is_deleted = 1 + deleted_at, not a hard delete.
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
