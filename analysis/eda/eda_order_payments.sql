USE OlistDWH;

-- EDA: raw.order_payments

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.order_payments
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.order_payments
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.order_payments
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'             AS column_name, LEN(order_id)             AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_sequential'   AS column_name, LEN(payment_sequential)   AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_type'         AS column_name, LEN(payment_type)         AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_installments' AS column_name, LEN(payment_installments) AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_value'        AS column_name, LEN(payment_value)        AS length FROM raw.order_payments WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'order_id'             AS column_name, LEN(TRIM(REPLACE(order_id,       '"', ''))) AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_sequential'   AS column_name, LEN(TRIM(payment_sequential))               AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_type'         AS column_name, LEN(TRIM(REPLACE(payment_type,   '"', ''))) AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_installments' AS column_name, LEN(TRIM(payment_installments))             AS length FROM raw.order_payments WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'payment_value'        AS column_name, LEN(TRIM(payment_value))                    AS length FROM raw.order_payments WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN order_id             IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN payment_sequential   IS NULL THEN 1 ELSE 0 END) AS null_payment_sequential,
    SUM(CASE WHEN payment_type         IS NULL THEN 1 ELSE 0 END) AS null_payment_type,
    SUM(CASE WHEN payment_installments IS NULL THEN 1 ELSE 0 END) AS null_payment_installments,
    SUM(CASE WHEN payment_value        IS NULL THEN 1 ELSE 0 END) AS null_payment_value
FROM raw.order_payments
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis
SELECT
    SUM(CASE WHEN TRIM(order_id)             = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN TRIM(payment_sequential)   = '' THEN 1 ELSE 0 END) AS empty_payment_sequential,
    SUM(CASE WHEN TRIM(payment_type)         = '' THEN 1 ELSE 0 END) AS empty_payment_type,
    SUM(CASE WHEN TRIM(payment_installments) = '' THEN 1 ELSE 0 END) AS empty_payment_installments,
    SUM(CASE WHEN TRIM(payment_value)        = '' THEN 1 ELSE 0 END) AS empty_payment_value
FROM raw.order_payments
WHERE batch_id = @batch_id;


-- 6. Numeric Parse Failures
SELECT
    SUM(CASE WHEN payment_sequential   IS NOT NULL AND TRY_CAST(TRIM(payment_sequential)   AS INT)          IS NULL THEN 1 ELSE 0 END) AS sequential_parse_failures,
    SUM(CASE WHEN payment_installments IS NOT NULL AND TRY_CAST(TRIM(payment_installments) AS INT)          IS NULL THEN 1 ELSE 0 END) AS installments_parse_failures,
    SUM(CASE WHEN payment_value        IS NOT NULL AND TRY_CAST(TRIM(payment_value)        AS DECIMAL(10,2)) IS NULL THEN 1 ELSE 0 END) AS value_parse_failures
FROM raw.order_payments
WHERE batch_id = @batch_id;


-- 7. Payment Type Distribution
SELECT
    payment_type,
    COUNT(*)                                           AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.order_payments
WHERE batch_id = @batch_id
GROUP BY payment_type
ORDER BY cnt DESC;


-- 8. Payment Value Statistics
SELECT
    MIN(val)                    AS min_value_payment,
    MAX(val)                    AS max_value_payment,
    AVG(val)                    AS avg_value_payment,
    MAX(median_val)             AS median_value_payment
FROM (
    SELECT
        val,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY val) OVER (), 2) AS median_val
    FROM (
        SELECT TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2)) AS val
        FROM raw.order_payments
        WHERE batch_id = @batch_id
          AND TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2)) IS NOT NULL
    ) AS converted
) AS computed;


-- 9. Outliers: Zero or Negative Payment Value
SELECT COUNT(*) AS zero_or_negative_payment_value
FROM raw.order_payments
WHERE batch_id = @batch_id
  AND TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2)) <= 0;


-- 9a. Sample Rows: Zero or Negative Payment Value
SELECT
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value AS zero_or_negative_payment_value
FROM raw.order_payments
WHERE batch_id = @batch_id
  AND TRY_CAST(TRIM(payment_value) AS DECIMAL(10,2)) <= 0;


-- 10. Installments Distribution
SELECT
    TRY_CAST(TRIM(payment_installments) AS INT) AS installments,
    COUNT(*)                                    AS cnt
FROM raw.order_payments
WHERE batch_id = @batch_id
  AND TRY_CAST(TRIM(payment_installments) AS INT) IS NOT NULL
GROUP BY TRY_CAST(TRIM(payment_installments) AS INT)
ORDER BY installments;


-- 11. Orders with Multiple Payment Methods
SELECT
    order_id,
    COUNT(DISTINCT payment_type)                                        AS distinct_payment_types,
    STRING_AGG(payment_type, ', ') WITHIN GROUP (ORDER BY payment_type) AS payment_types,
    COUNT(*)                                                            AS payment_rows
FROM raw.order_payments
WHERE batch_id = @batch_id
GROUP BY order_id
HAVING COUNT(DISTINCT payment_type) > 1
ORDER BY distinct_payment_types DESC;


-- 12. Referential Integrity: payments referencing unknown orders
SELECT COUNT(*) AS payments_without_matching_order
FROM raw.order_payments p
WHERE p.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.orders o WHERE o.order_id = p.order_id
);
