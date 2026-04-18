USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_dim_date
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL,
    @start_date  DATE             = '2016-01-01',
    @end_date    DATE             = '2025-12-31'   -- extend as needed
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @rows        INT       = 0;
    DECLARE @start_time  DATETIME2 = SYSUTCDATETIME();
    DECLARE @duration_ms INT;
    DECLARE @error_msg   NVARCHAR(MAX);
    DECLARE @log_id      INT;

    INSERT INTO audit.load_log (
        job_run_id,  pipeline_id,
        layer,       sp_name,     table_name,
        rows_processed, status,  load_ts
    )
    VALUES (
        @job_run_id, @pipeline_id,
        'MART',      'mart.sp_load_dim_date', 'mart.dim_date',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Unknown / missing date sentinel row.
        -- Used when a date FK cannot be resolved (should not occur with valid data).
        IF NOT EXISTS (SELECT 1 FROM mart.dim_date WHERE date_key = 0)
        BEGIN
            INSERT INTO mart.dim_date (date_key, full_date, year, iso_year, quarter, month, month_name, week_of_year, day_of_month, day_of_week, day_name, is_weekend)
            VALUES (0, '1900-01-01', 1900, 1900, 1, 1, 'Unknown', 1, 1, 0, 'Unknown', 0);
        END

        -- Tally CTE generates sequential integers 0..9999 (≈27 years of daily rows).
        -- month_name / day_name are hardcoded English strings to be locale-independent.
        -- One row per date between @start_date and @end_date (inclusive).
        ;WITH
        E1(n)    AS (SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1
                     UNION ALL SELECT 1 UNION ALL SELECT 1 UNION ALL SELECT 1),  -- 10
        E2(n)    AS (SELECT 1 FROM E1 a CROSS JOIN E1 b),                        -- 100
        E3(n)    AS (SELECT 1 FROM E2 a CROSS JOIN E2 b),                        -- 10,000
        Tally(n) AS (SELECT ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) - 1 FROM E3),
        Dates(d) AS (
            SELECT CAST(DATEADD(DAY, n, @start_date) AS DATE)
            FROM Tally
            WHERE DATEADD(DAY, n, @start_date) <= @end_date
        )
        INSERT INTO mart.dim_date (
            date_key,
            full_date,
            year,
            iso_year,
            quarter,
            month,
            month_name,
            week_of_year,
            day_of_month,
            day_of_week,
            day_name,
            is_weekend
        )
        SELECT
            YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)   AS date_key,
            d                                             AS full_date,
            CAST(YEAR(d) AS SMALLINT)                    AS year,
            CAST(
                CASE
                    WHEN MONTH(d) = 1  AND DATEPART(ISO_WEEK, d) >= 52 THEN YEAR(d) - 1
                    WHEN MONTH(d) = 12 AND DATEPART(ISO_WEEK, d) = 1   THEN YEAR(d) + 1
                    ELSE YEAR(d)
                END
            AS SMALLINT)                                 AS iso_year,
            CAST(DATEPART(QUARTER, d) AS TINYINT)        AS quarter,
            CAST(MONTH(d) AS TINYINT)                    AS month,
            CAST(
                CASE MONTH(d)
                    WHEN  1 THEN 'January'   WHEN  2 THEN 'February'
                    WHEN  3 THEN 'March'     WHEN  4 THEN 'April'
                    WHEN  5 THEN 'May'       WHEN  6 THEN 'June'
                    WHEN  7 THEN 'July'      WHEN  8 THEN 'August'
                    WHEN  9 THEN 'September' WHEN 10 THEN 'October'
                    WHEN 11 THEN 'November'  WHEN 12 THEN 'December'
                END
            AS NVARCHAR(9))                              AS month_name,
            CAST(DATEPART(ISO_WEEK, d) AS TINYINT)       AS week_of_year,
            CAST(DAY(d) AS TINYINT)                      AS day_of_month,
            CAST(DATEPART(WEEKDAY, d) AS TINYINT)        AS day_of_week,
            CAST(
                CASE DATEPART(WEEKDAY, d)  -- DATEFIRST=7: 1=Sun,2=Mon,...,7=Sat
                    WHEN 1 THEN 'Sunday'    WHEN 2 THEN 'Monday'
                    WHEN 3 THEN 'Tuesday'   WHEN 4 THEN 'Wednesday'
                    WHEN 5 THEN 'Thursday'  WHEN 6 THEN 'Friday'
                    WHEN 7 THEN 'Saturday'
                END
            AS NVARCHAR(9))                              AS day_name,
            CAST(
                CASE WHEN DATEPART(WEEKDAY, d) IN (1, 7) THEN 1 ELSE 0 END
            AS BIT)                                      AS is_weekend
        FROM Dates
        -- Dates already present are skipped — idempotent, safe to re-run.
        WHERE NOT EXISTS (
            SELECT 1 FROM mart.dim_date
            WHERE date_key = YEAR(d) * 10000 + MONTH(d) * 100 + DAY(d)
        );

        SET @rows        = @@ROWCOUNT;
        SET @duration_ms = DATEDIFF(MILLISECOND, @start_time, SYSUTCDATETIME());

        UPDATE audit.load_log
        SET rows_processed        = @rows,
            status                = 'SUCCESS',
            processed_duration_ms = @duration_ms
        WHERE log_id = @log_id;

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
        WHERE log_id = @log_id;

        INSERT INTO audit.error_log (
            job_run_id,  pipeline_id, sp_name,
            error_message, error_ts,
            error_severity, error_procedure, error_line
        )
        VALUES (
            @job_run_id, @pipeline_id, 'mart.sp_load_dim_date',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
