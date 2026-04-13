USE OlistDWH;

-- EDA: raw.sellers

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.sellers
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.sellers
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.sellers
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'seller_id'              AS column_name, LEN(seller_id)              AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_zip_code_prefix' AS column_name, LEN(seller_zip_code_prefix) AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_city'            AS column_name, LEN(seller_city)            AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_state'           AS column_name, LEN(seller_state)           AS length FROM raw.sellers WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'seller_id'              AS column_name, LEN(TRIM(REPLACE(seller_id,              '"', ''))) AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_zip_code_prefix' AS column_name, LEN(TRIM(REPLACE(seller_zip_code_prefix, '"', ''))) AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_city'            AS column_name, LEN(TRIM(seller_city))                               AS length FROM raw.sellers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_state'           AS column_name, LEN(TRIM(seller_state))                              AS length FROM raw.sellers WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN seller_id IS NULL              THEN 1 ELSE 0 END) AS null_seller_id,
    SUM(CASE WHEN seller_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code_prefix,
    SUM(CASE WHEN seller_city IS NULL            THEN 1 ELSE 0 END) AS null_seller_city,
    SUM(CASE WHEN seller_state IS NULL           THEN 1 ELSE 0 END) AS null_seller_state
FROM raw.sellers
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(REPLACE(seller_id,              '"', '')) = '' THEN 1 ELSE 0 END) AS empty_seller_id,
    SUM(CASE WHEN TRIM(REPLACE(seller_zip_code_prefix, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_zip_code_prefix,
    SUM(CASE WHEN TRIM(seller_city)  = ''                              THEN 1 ELSE 0 END) AS empty_seller_city,
    SUM(CASE WHEN TRIM(seller_state) = ''                              THEN 1 ELSE 0 END) AS empty_seller_state
FROM raw.sellers
WHERE batch_id = @batch_id;


-- 6. Duplicate seller_id
SELECT seller_id, COUNT(*) AS duplicate_cnt
FROM raw.sellers
WHERE batch_id = @batch_id
GROUP BY seller_id
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 7. Zip Code Format Check (expected: 5 numeric digits)
SELECT COUNT(*) AS invalid_zip_format
FROM raw.sellers
WHERE batch_id = @batch_id
  AND TRIM(REPLACE(seller_zip_code_prefix, '"', '')) NOT LIKE '[0-9][0-9][0-9][0-9][0-9]';


-- 8. Distribution by State
SELECT
    seller_state,
    COUNT(*)                                           AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.sellers
WHERE batch_id = @batch_id
GROUP BY seller_state
ORDER BY cnt DESC;


-- 9. Top 10 Cities by Seller Count
SELECT TOP 10
    seller_city,
    seller_state,
    COUNT(*) AS cnt
FROM raw.sellers
WHERE batch_id = @batch_id
GROUP BY seller_city, seller_state
ORDER BY cnt DESC;


-- 10. Outliers: Cities with Inconsistent State Assignment
SELECT
    seller_city,
    COUNT(DISTINCT seller_state)                                          AS distinct_states,
    STRING_AGG(seller_state, ', ') WITHIN GROUP (ORDER BY seller_state)  AS states
FROM (
    SELECT DISTINCT seller_city, seller_state
    FROM raw.sellers
    WHERE batch_id = @batch_id
) AS deduped
GROUP BY seller_city
HAVING COUNT(DISTINCT seller_state) > 1
ORDER BY distinct_states DESC;


-- 11. Referential Integrity: sellers not referenced in order_items
SELECT COUNT(*) AS sellers_without_order_items
FROM raw.sellers s
WHERE s.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.order_items oi WHERE oi.seller_id = s.seller_id
);
