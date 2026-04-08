USE OlistDWH;

-- EDA: raw.orders

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.orders
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.orders
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.orders
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'                      AS column_name, LEN(order_id)                      AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_id'                   AS column_name, LEN(customer_id)                   AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_status'                  AS column_name, LEN(order_status)                  AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_purchase_timestamp'      AS column_name, LEN(order_purchase_timestamp)      AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_approved_at'             AS column_name, LEN(order_approved_at)             AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_delivered_carrier_date'  AS column_name, LEN(order_delivered_carrier_date)  AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_delivered_customer_date' AS column_name, LEN(order_delivered_customer_date) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_estimated_delivery_date' AS column_name, LEN(order_estimated_delivery_date) AS length FROM raw.orders WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'order_id'                      AS column_name, LEN(TRIM(REPLACE(order_id,    '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_id'                   AS column_name, LEN(TRIM(REPLACE(customer_id, '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_status'                  AS column_name, LEN(TRIM(REPLACE(order_status,                  '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_purchase_timestamp'      AS column_name, LEN(TRIM(REPLACE(order_purchase_timestamp,      '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_approved_at'             AS column_name, LEN(TRIM(REPLACE(order_approved_at,             '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_delivered_carrier_date'  AS column_name, LEN(TRIM(REPLACE(order_delivered_carrier_date,  '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_delivered_customer_date' AS column_name, LEN(TRIM(REPLACE(order_delivered_customer_date, '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_estimated_delivery_date' AS column_name, LEN(TRIM(REPLACE(order_estimated_delivery_date, '"', ''))) AS length FROM raw.orders WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)                      AS null_order_id,
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)                   AS null_customer_id,
    SUM(CASE WHEN order_status IS NULL THEN 1 ELSE 0 END)                  AS null_order_status,
    SUM(CASE WHEN order_purchase_timestamp IS NULL THEN 1 ELSE 0 END)      AS null_purchase_timestamp,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END)             AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END)  AS null_delivered_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_customer_date,
    SUM(CASE WHEN order_estimated_delivery_date IS NULL THEN 1 ELSE 0 END) AS null_estimated_delivery_date
FROM raw.orders
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(REPLACE(order_id,    '"', '')) = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN TRIM(REPLACE(customer_id, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_customer_id,
    SUM(CASE WHEN TRIM(order_status) = ''               THEN 1 ELSE 0 END)    AS empty_order_status
FROM raw.orders
WHERE batch_id = @batch_id;


-- 6. Date Parse Failures (TRY_CONVERT returns NULL on malformed input)
SELECT
    SUM(CASE WHEN order_purchase_timestamp IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_purchase_ts,
    SUM(CASE WHEN order_approved_at IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(order_approved_at), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_carrier_date), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_customer_date,
    SUM(CASE WHEN order_estimated_delivery_date IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(order_estimated_delivery_date), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_estimated_date
FROM raw.orders
WHERE batch_id = @batch_id;


-- 7. Duplicate order_id
SELECT order_id, COUNT(*) AS duplicate_cnt
FROM raw.orders
WHERE batch_id = @batch_id
GROUP BY order_id
HAVING COUNT(*) > 1;


-- 8. Order Status Distribution
SELECT
    order_status,
    COUNT(*)                                                  AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)        AS pct
FROM raw.orders
WHERE batch_id = @batch_id
GROUP BY order_status
ORDER BY cnt DESC;


-- 9. Order Date Range
SELECT
    MIN(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120)) AS earliest_order,
    MAX(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120)) AS latest_order,
    DATEDIFF(
        DAY,
        MIN(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120)),
        MAX(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120))
    )                                                                    AS date_range_days
FROM raw.orders
WHERE batch_id = @batch_id;


-- 10. Null Dates by Order Status
SELECT
    order_status,
    COUNT(*) AS total,
    SUM(CASE WHEN order_approved_at IS NULL THEN 1 ELSE 0 END)             AS null_approved_at,
    SUM(CASE WHEN order_delivered_carrier_date IS NULL THEN 1 ELSE 0 END)  AS null_delivered_carrier_date,
    SUM(CASE WHEN order_delivered_customer_date IS NULL THEN 1 ELSE 0 END) AS null_delivered_customer_date
FROM raw.orders
WHERE batch_id = @batch_id
GROUP BY order_status
ORDER BY total DESC;


-- 11. Logical Consistency: Date Chain Violations
-- carrier before purchase?
SELECT COUNT(*) AS carrier_before_purchase
FROM raw.orders
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_carrier_date), 120) <
      TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120);

-- customer delivery before carrier handoff?
SELECT COUNT(*) AS customer_before_carrier
FROM raw.orders
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) <
      TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_carrier_date), 120);

-- customer delivery before purchase?
SELECT COUNT(*) AS customer_before_purchase
FROM raw.orders
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) <
      TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120);

-- delivered status but no customer delivery date?
SELECT COUNT(*) AS delivered_without_date
FROM raw.orders
WHERE batch_id = @batch_id
  AND order_status = 'delivered'
  AND order_delivered_customer_date IS NULL;

-- non-delivered status but customer delivery date present?
SELECT COUNT(*) AS date_without_delivered_status
FROM raw.orders
WHERE batch_id = @batch_id
  AND order_status != 'delivered'
  AND order_delivered_customer_date IS NOT NULL;


-- 12. Delivery Time Analysis (delivered orders only)
SELECT
    MIN(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120),
        TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120))) AS min_delivery_days,
    MAX(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120),
        TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120))) AS max_delivery_days,
    AVG(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120),
        TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120))) AS avg_delivery_days
FROM raw.orders
WHERE batch_id = @batch_id
  AND order_status = 'delivered'
  AND TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) IS NOT NULL;


-- 13. Late Deliveries (delivered after estimated date)
SELECT
    SUM(CASE WHEN TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) >
                  TRY_CONVERT(DATETIME2(0), TRIM(order_estimated_delivery_date), 120)
             THEN 1 ELSE 0 END)                                           AS late_deliveries,
    ROUND(
        SUM(CASE WHEN TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120) >
                      TRY_CONVERT(DATETIME2(0), TRIM(order_estimated_delivery_date), 120)
                 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 2
    )                                                                     AS pct_late
FROM raw.orders
WHERE batch_id = @batch_id
  AND order_status = 'delivered';


-- 14. Outliers: Suspicious Delivery Times (> 60 days)
SELECT TOP 10
    order_id,
    order_purchase_timestamp,
    order_delivered_customer_date,
    DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120),
        TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120)) AS delivery_days_outliers
FROM raw.orders
WHERE batch_id = @batch_id
  AND DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120),
        TRY_CONVERT(DATETIME2(0), TRIM(order_delivered_customer_date), 120)) > 60
ORDER BY delivery_days_outliers DESC;


-- 15. Orders per Month
SELECT
    FORMAT(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120), 'yyyy-MM') AS year_month,
    COUNT(*) AS total_orders
FROM raw.orders
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120) IS NOT NULL
GROUP BY FORMAT(TRY_CONVERT(DATETIME2(0), TRIM(order_purchase_timestamp), 120), 'yyyy-MM')
ORDER BY year_month;


-- 16. Referential Integrity: orders without a matching customer
SELECT COUNT(*) AS orders_without_customer
FROM raw.orders o
WHERE o.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.customers c WHERE c.customer_id = o.customer_id
);
