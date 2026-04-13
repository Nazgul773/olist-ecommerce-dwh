USE OlistDWH;

-- EDA: raw.order_reviews

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.order_reviews
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'review_id'               AS column_name, LEN(review_id)               AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_id'                AS column_name, LEN(order_id)                AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_score'            AS column_name, LEN(review_score)            AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_comment_title'    AS column_name, LEN(review_comment_title)    AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_comment_message'  AS column_name, LEN(review_comment_message)  AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_creation_date'    AS column_name, LEN(review_creation_date)    AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_answer_timestamp' AS column_name, LEN(review_answer_timestamp) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'review_id'               AS column_name, LEN(TRIM(REPLACE(review_id,               '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_id'                AS column_name, LEN(TRIM(REPLACE(order_id,                '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_score'            AS column_name, LEN(TRIM(REPLACE(review_score,            '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_comment_title'    AS column_name, LEN(TRIM(REPLACE(review_comment_title,    '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_comment_message'  AS column_name, LEN(TRIM(REPLACE(review_comment_message,  '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_creation_date'    AS column_name, LEN(TRIM(REPLACE(review_creation_date,    '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'review_answer_timestamp' AS column_name, LEN(TRIM(REPLACE(review_answer_timestamp, '"', ''))) AS length FROM raw.order_reviews WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN review_id               IS NULL THEN 1 ELSE 0 END) AS null_review_id,
    SUM(CASE WHEN order_id                IS NULL THEN 1 ELSE 0 END) AS null_order_id,
    SUM(CASE WHEN review_score            IS NULL THEN 1 ELSE 0 END) AS null_review_score,
    SUM(CASE WHEN review_comment_title    IS NULL THEN 1 ELSE 0 END) AS null_comment_title,
    SUM(CASE WHEN review_comment_message  IS NULL THEN 1 ELSE 0 END) AS null_comment_message,
    SUM(CASE WHEN review_creation_date    IS NULL THEN 1 ELSE 0 END) AS null_creation_date,
    SUM(CASE WHEN review_answer_timestamp IS NULL THEN 1 ELSE 0 END) AS null_answer_timestamp
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after trimming)
SELECT
    SUM(CASE WHEN TRIM(review_id)               = '' THEN 1 ELSE 0 END) AS empty_review_id,
    SUM(CASE WHEN TRIM(order_id)                = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN TRIM(review_score)            = '' THEN 1 ELSE 0 END) AS empty_review_score,
    SUM(CASE WHEN TRIM(review_comment_title)    = '' THEN 1 ELSE 0 END) AS empty_comment_title,
    SUM(CASE WHEN TRIM(review_comment_message)  = '' THEN 1 ELSE 0 END) AS empty_comment_message,
    SUM(CASE WHEN TRIM(review_creation_date)    = '' THEN 1 ELSE 0 END) AS empty_creation_date,
    SUM(CASE WHEN TRIM(review_answer_timestamp) = '' THEN 1 ELSE 0 END) AS empty_answer_timestamp
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 6. Date Parse Failures
SELECT
    SUM(CASE WHEN review_creation_date    IS NOT NULL AND TRY_CONVERT(DATETIME2, TRIM(review_creation_date))    IS NULL THEN 1 ELSE 0 END) AS creation_date_parse_failures,
    SUM(CASE WHEN review_answer_timestamp IS NOT NULL AND TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp)) IS NULL THEN 1 ELSE 0 END) AS answer_timestamp_parse_failures
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 7. Duplicate review_id
SELECT review_id, COUNT(*) AS duplicate_cnt
FROM raw.order_reviews
WHERE batch_id = @batch_id
GROUP BY review_id
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 8. Review Score Distribution
SELECT
    review_score,
    COUNT(*)                                           AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.order_reviews
WHERE batch_id = @batch_id
GROUP BY review_score
ORDER BY review_score;


-- 9. Date Range
SELECT
    MIN(TRY_CONVERT(DATETIME2, TRIM(review_creation_date)))    AS min_creation_date,
    MAX(TRY_CONVERT(DATETIME2, TRIM(review_creation_date)))    AS max_creation_date,
    MIN(TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp))) AS min_answer_timestamp,
    MAX(TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp))) AS max_answer_timestamp
FROM raw.order_reviews
WHERE batch_id = @batch_id;


-- 10. Avg Response Time (days from creation to answer)
SELECT
    AVG(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2, TRIM(review_creation_date)),
        TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp))
    )) AS avg_response_days,
    MIN(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2, TRIM(review_creation_date)),
        TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp))
    )) AS min_response_days,
    MAX(DATEDIFF(DAY,
        TRY_CONVERT(DATETIME2, TRIM(review_creation_date)),
        TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp))
    )) AS max_response_days
FROM raw.order_reviews
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DATETIME2, TRIM(review_creation_date))    IS NOT NULL
  AND TRY_CONVERT(DATETIME2, TRIM(review_answer_timestamp)) IS NOT NULL;


-- 11. Referential Integrity: reviews referencing unknown orders
SELECT COUNT(*) AS reviews_without_matching_order
FROM raw.order_reviews r
WHERE r.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.orders o WHERE o.order_id = r.order_id
);
