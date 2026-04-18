USE OlistDWH;
GO

CREATE OR ALTER PROCEDURE mart.sp_load_fact_sales
    @pipeline_id INT              = NULL,
    @job_run_id  UNIQUEIDENTIFIER = NULL
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
        'MART',      'mart.sp_load_fact_sales', 'mart.fact_sales',
        0,           'RUNNING',   SYSUTCDATETIME()
    );
    SET @log_id = SCOPE_IDENTITY();

    BEGIN TRY
        -- Full reload: TRUNCATE + INSERT in a transaction. TRUNCATE is rollback-safe in SQL Server.
        -- IDENTITY seed resets each run — acceptable, sales_key is a technical PK only.
        BEGIN TRANSACTION;

        TRUNCATE TABLE mart.fact_sales;

        -- dim_date keys are computed inline using YYYYMMDD integer arithmetic,
        -- matching the date_key format in mart.dim_date.
        -- ISNULL fallback to 0 (unknown sentinel) for any date not in dim_date —
        -- should not occur within the 2016–2025 range seeded by sp_load_dim_date.
        -- ISNULL fallback to -1 (unknown member) for dimension FKs where the
        -- natural key cannot be resolved (data quality gap, not an ETL error).
        --
        -- review_score: orders can have at most one review. A small subset of orders
        -- have duplicate review rows in the source — the CTE keeps the most recent.
        -- The score is duplicated across all items of the same order
        -- Use AVERAGE (not SUM) for aggregations.
        --
        -- delivery_vs_estimate_days: negative = delivered earlier than estimated (good),
        -- positive = delivered late.
        ;WITH latest_review AS (
            SELECT
                order_id,
                review_score,
                ROW_NUMBER() OVER (
                    PARTITION BY order_id
                    ORDER BY review_creation_date DESC, review_answer_timestamp DESC
                ) AS rn
            FROM cleansed.order_reviews
            WHERE is_deleted = 0
        )
        INSERT INTO mart.fact_sales (
            purchase_date_key,
            estimated_delivery_date_key,
            carrier_handoff_date_key,
            actual_delivery_date_key,
            customer_key,
            seller_key,
            product_key,
            order_status_key,
            order_id,
            order_item_id,
            price,
            freight_value,
            total_value,
            purchase_to_delivery_days,
            delivery_vs_estimate_days,
            purchase_to_approval_hours,
            carrier_to_delivery_days,
            review_score
        )
        SELECT
            ISNULL(dd_pur.date_key,  0) AS purchase_date_key,
            ISNULL(dd_est.date_key,  0) AS estimated_delivery_date_key,
            dd_car.date_key             AS carrier_handoff_date_key,  -- NULL if not yet handed off
            dd_del.date_key             AS actual_delivery_date_key,  -- NULL if not yet delivered

            -- Dimension surrogate key lookups (-1 = unknown member)
            ISNULL(dc.customer_key,   -1) AS customer_key,
            ISNULL(ds.seller_key,     -1) AS seller_key,
            ISNULL(dp.product_key,    -1) AS product_key,
            ISNULL(dos.order_status_key, -1) AS order_status_key,

            -- Degenerate dimensions
            oi.order_id,
            CAST(oi.order_item_id AS INT) AS order_item_id,

            oi.price,
            oi.freight_value,
            oi.price + oi.freight_value  AS total_value,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                THEN DATEDIFF(DAY, o.order_purchase_timestamp, o.order_delivered_customer_date)
            END AS purchase_to_delivery_days,

            CASE
                WHEN o.order_delivered_customer_date IS NOT NULL
                THEN DATEDIFF(DAY, o.order_estimated_delivery_date, o.order_delivered_customer_date)
            END AS delivery_vs_estimate_days,

            CASE
                WHEN o.order_approved_at IS NOT NULL
                THEN DATEDIFF(HOUR, o.order_purchase_timestamp, o.order_approved_at)
            END AS purchase_to_approval_hours,

            CASE
                WHEN o.order_delivered_carrier_date  IS NOT NULL
                 AND o.order_delivered_customer_date IS NOT NULL
                THEN DATEDIFF(DAY, o.order_delivered_carrier_date, o.order_delivered_customer_date)
            END AS carrier_to_delivery_days,

            -- Review score (order-level, duplicated across item rows)
            r.review_score

        FROM cleansed.order_items oi

        -- Orders: provides dates, status, customer_id
        JOIN cleansed.orders o
            ON  o.order_id   = oi.order_id
            AND o.is_deleted = 0

        -- Date dimension: purchase date (role-playing — purchase)
        LEFT JOIN mart.dim_date dd_pur
            ON dd_pur.date_key =   YEAR(o.order_purchase_timestamp)      * 10000
                                 + MONTH(o.order_purchase_timestamp)     * 100
                                 + DAY(o.order_purchase_timestamp)

        -- Date dimension: estimated delivery (role-playing — SLA target)
        LEFT JOIN mart.dim_date dd_est
            ON dd_est.date_key =   YEAR(o.order_estimated_delivery_date)  * 10000
                                 + MONTH(o.order_estimated_delivery_date) * 100
                                 + DAY(o.order_estimated_delivery_date)

        -- Date dimension: carrier handoff (role-playing — NULL if not yet handed off)
        LEFT JOIN mart.dim_date dd_car
            ON o.order_delivered_carrier_date IS NOT NULL
            AND dd_car.date_key =   YEAR(o.order_delivered_carrier_date)  * 10000
                                  + MONTH(o.order_delivered_carrier_date) * 100
                                  + DAY(o.order_delivered_carrier_date)

        -- Date dimension: actual delivery (role-playing — NULL if undelivered)
        LEFT JOIN mart.dim_date dd_del
            ON o.order_delivered_customer_date IS NOT NULL
            AND dd_del.date_key =   YEAR(o.order_delivered_customer_date)  * 10000
                                  + MONTH(o.order_delivered_customer_date) * 100
                                  + DAY(o.order_delivered_customer_date)

        LEFT JOIN mart.dim_customer dc
            ON dc.customer_id = o.customer_id

        LEFT JOIN mart.dim_seller ds
            ON ds.seller_id = oi.seller_id

        LEFT JOIN mart.dim_product dp
            ON dp.product_id = oi.product_id

        LEFT JOIN mart.dim_order_status dos
            ON dos.status_name = o.order_status

        -- Review (latest per order, left join — not all orders have a review)
        LEFT JOIN latest_review r
            ON  r.order_id = oi.order_id
            AND r.rn       = 1

        WHERE oi.is_deleted = 0;

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
            @job_run_id, @pipeline_id, 'mart.sp_load_fact_sales',
            @error_msg,    SYSUTCDATETIME(),
            ERROR_SEVERITY(), ERROR_PROCEDURE(), ERROR_LINE()
        );

        THROW;
    END CATCH
END;
GO
