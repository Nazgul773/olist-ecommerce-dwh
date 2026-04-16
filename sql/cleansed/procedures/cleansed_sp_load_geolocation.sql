USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE cleansed.sp_load_geolocation
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
            'CLEANSED',     'cleansed.sp_load_geolocation', 'cleansed.geolocation',
            0,              'RUNNING',   SYSUTCDATETIME()
        );

        -- 1. Normalize raw data into a temp table so DQ checks and the MERGE
        --    share the same cleaned values without duplicating transformation logic.
        --    Two-stage CTE:
        --      'normalized' — computes cleaned values once (incl. UDF calls).
        --      'ranked'     — assigns ROW_NUMBER() for deterministic zip selection. row_hash is
        --                     computed inline in the MERGE — no Type A/B handling (multiple coordinates
        --                     per zip is an expected dataset characteristic, not a DQ conflict).
        ;WITH normalized AS (
            SELECT
                geolocation_zip_code_prefix,
                geolocation_lat,                                                     -- raw value retained for DQ NULL/parse checks
                geolocation_lng,                                                     -- raw value retained for DQ NULL/parse checks
                REPLACE(TRIM(geolocation_zip_code_prefix), '"', '')                 AS clean_zip,
                TRY_CONVERT(DECIMAL(10,7), REPLACE(TRIM(geolocation_lat), '"', '')) AS parsed_lat,
                TRY_CONVERT(DECIMAL(10,7), REPLACE(TRIM(geolocation_lng), '"', '')) AS parsed_lng,
                dbo.fn_normalize_text(REPLACE(geolocation_city,  '"', ''))          AS clean_city,
                dbo.fn_normalize_text(REPLACE(geolocation_state, '"', ''))          AS clean_state
            FROM raw.geolocation
            WHERE batch_id = @batch_id
        ),
        ranked AS (
            SELECT
                *,
                ROW_NUMBER() OVER (
                    PARTITION BY clean_zip
                    ORDER BY parsed_lat, parsed_lng, clean_city
                ) AS rn
            FROM normalized
        )
        SELECT
            geolocation_zip_code_prefix, geolocation_lat, geolocation_lng,
            clean_zip, parsed_lat, parsed_lng, clean_city, clean_state, rn
        INTO #normalized_geolocation
        FROM ranked;

        -- 2. DQ checks: completeness, validity (length + format + range), uniqueness.
        --    One dq_log row per distinct (column_name, issue) category with affected_row_count.
        WITH dq_checks AS (

            -- Completeness: NULL checks
            SELECT 'geolocation_zip_code_prefix' AS column_name, 'NULL value' AS issue FROM #normalized_geolocation WHERE clean_zip      IS NULL
            UNION ALL
            SELECT 'geolocation_lat',             'NULL value'                          FROM #normalized_geolocation WHERE geolocation_lat IS NULL
            UNION ALL
            SELECT 'geolocation_lng',             'NULL value'                          FROM #normalized_geolocation WHERE geolocation_lng IS NULL
            UNION ALL
            SELECT 'geolocation_city',            'NULL value'                          FROM #normalized_geolocation WHERE clean_city      IS NULL
            UNION ALL
            SELECT 'geolocation_state',           'NULL value'                          FROM #normalized_geolocation WHERE clean_state     IS NULL

            -- Completeness: empty string after cleansing
            UNION ALL
            SELECT 'geolocation_zip_code_prefix', 'Empty string after cleansing'        FROM #normalized_geolocation WHERE clean_zip   = ''
            UNION ALL
            SELECT 'geolocation_city',            'Empty string after cleansing'        FROM #normalized_geolocation WHERE clean_city  = ''
            UNION ALL
            SELECT 'geolocation_state',           'Empty string after cleansing'        FROM #normalized_geolocation WHERE clean_state = ''

            -- Validity: format and length checks
            UNION ALL
            SELECT 'geolocation_zip_code_prefix', 'Invalid length or format: expected 5 numeric digits' FROM #normalized_geolocation WHERE clean_zip != '' AND (LEN(clean_zip) != 5 OR clean_zip LIKE '%[^0-9]%')

            UNION ALL
            SELECT 'geolocation_state', 'Invalid length or format: expected 2 uppercase letters'  FROM #normalized_geolocation WHERE clean_state != '' AND (LEN(clean_state) != 2 OR clean_state LIKE '%[^A-Z]%')

            -- Validity: coordinate parse failures
            UNION ALL
            SELECT 'geolocation_lat', 'Invalid decimal format' FROM #normalized_geolocation WHERE geolocation_lat IS NOT NULL AND parsed_lat IS NULL
            UNION ALL
            SELECT 'geolocation_lng', 'Invalid decimal format' FROM #normalized_geolocation WHERE geolocation_lng IS NOT NULL AND parsed_lng IS NULL

            -- Duplicates: rn>1 rows excluded from MERGE
            UNION ALL
            SELECT 'geolocation_zip_code_prefix', 'Duplicate zip_code_prefix: kept smallest (lat, lng) occurrence' FROM #normalized_geolocation WHERE rn > 1

        )

        INSERT INTO audit.dq_log (batch_id, job_run_id, table_name, column_name, issue, affected_row_count)
        SELECT @batch_id, @job_run_id, 'geolocation', column_name, issue, COUNT(*)
        FROM dq_checks
        GROUP BY column_name, issue;

        -- 3. Incremental upsert + soft delete via MERGE. row_hash is pre-computed in the
        --    temp table and reused here.
        --    The raw dataset contains multiple coordinate pairs per zip_code_prefix (expected)
        --    ; one is selected deterministically (ORDER BY lat, lng, city) as rn = 1
        --    ; cleansed stores one representative row per zip (PK).
        BEGIN TRANSACTION;
        ;WITH hashed AS (
            SELECT
                clean_zip,
                parsed_lat,
                parsed_lng,
                clean_city,
                clean_state,
                HASHBYTES('SHA2_256', CONCAT(
                    clean_zip,                     '|',
                    CAST(parsed_lat AS NVARCHAR),  '|',
                    CAST(parsed_lng AS NVARCHAR),  '|',
                    clean_city,                    '|',
                    clean_state
                )) AS row_hash
            FROM #normalized_geolocation
            WHERE rn = 1
        )
        MERGE cleansed.geolocation AS tgt
        USING (
            SELECT *
            FROM hashed
            WHERE clean_zip   IS NOT NULL  AND clean_zip != '' AND clean_zip NOT LIKE '%[^0-9]%'   AND LEN(clean_zip) = 5
              AND parsed_lat  IS NOT NULL
              AND parsed_lng  IS NOT NULL
              AND clean_city  IS NOT NULL AND clean_city  != ''
              AND clean_state IS NOT NULL AND clean_state != '' AND clean_state NOT LIKE '%[^A-Z]%' AND LEN(clean_state) = 2
        ) AS src
        ON  tgt.geolocation_zip_code_prefix = src.clean_zip
        -- Data changed (according to row_hash) or row is reactivating after a soft delete
        WHEN MATCHED AND (tgt.row_hash <> src.row_hash OR tgt.is_deleted = 1) THEN
            UPDATE SET
                geolocation_city  = src.clean_city,
                geolocation_state = src.clean_state,
                row_hash          = src.row_hash,
                is_deleted        = 0,
                deleted_at        = NULL,
                updated_at        = SYSUTCDATETIME()
        -- New row in current batch (source) that doesn't exist in cleansed (target)
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                geolocation_zip_code_prefix, geolocation_lat,   geolocation_lng,
                geolocation_city,            geolocation_state,
                row_hash,                    updated_at
            )
            VALUES (
                src.clean_zip,   src.parsed_lat,  src.parsed_lng,
                src.clean_city,  src.clean_state,
                src.row_hash,    SYSUTCDATETIME()
            )
        -- Row exists in cleansed (target) but not in current batch (source) — source no longer contains it
        -- Soft delete by marking is_deleted = 1 and setting deleted_at for historical tracking, instead of hard deleting.
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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_geolocation';

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
        WHERE batch_id = @batch_id AND sp_name = 'cleansed.sp_load_geolocation';

        INSERT INTO audit.error_log (
            batch_id,      job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @batch_id,     @job_run_id, @pipeline_id, 'cleansed.sp_load_geolocation',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
