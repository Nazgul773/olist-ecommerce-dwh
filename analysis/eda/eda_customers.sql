USE OlistDWH;

-- EDA: raw.customers

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.customers
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.customers
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.customers
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'customer_id'              AS column_name, LEN(customer_id)              AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_unique_id'       AS column_name, LEN(customer_unique_id)       AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_zip_code_prefix' AS column_name, LEN(customer_zip_code_prefix) AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_city'            AS column_name, LEN(customer_city)            AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_state'           AS column_name, LEN(customer_state)           AS length FROM raw.customers WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'customer_id'              AS column_name, LEN(TRIM(REPLACE(customer_id,              '"', ''))) AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_unique_id'       AS column_name, LEN(TRIM(REPLACE(customer_unique_id,       '"', ''))) AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_zip_code_prefix' AS column_name, LEN(TRIM(REPLACE(customer_zip_code_prefix, '"', ''))) AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_city'            AS column_name, LEN(TRIM(REPLACE(customer_city,            '"', ''))) AS length FROM raw.customers WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'customer_state'           AS column_name, LEN(TRIM(REPLACE(customer_state,           '"', ''))) AS length FROM raw.customers WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN customer_id IS NULL THEN 1 ELSE 0 END)              AS null_customer_id,
    SUM(CASE WHEN customer_unique_id IS NULL THEN 1 ELSE 0 END)       AS null_customer_unique_id,
    SUM(CASE WHEN customer_zip_code_prefix IS NULL THEN 1 ELSE 0 END) AS null_zip_code_prefix,
    SUM(CASE WHEN customer_city IS NULL THEN 1 ELSE 0 END)            AS null_customer_city,
    SUM(CASE WHEN customer_state IS NULL THEN 1 ELSE 0 END)           AS null_customer_state
FROM raw.customers
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(REPLACE(customer_id,              '"', '')) = '' THEN 1 ELSE 0 END) AS empty_customer_id,
    SUM(CASE WHEN TRIM(REPLACE(customer_unique_id,       '"', '')) = '' THEN 1 ELSE 0 END) AS empty_customer_unique_id,
    SUM(CASE WHEN TRIM(REPLACE(customer_zip_code_prefix, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_zip_code_prefix,
    SUM(CASE WHEN TRIM(REPLACE(customer_city,  '"', '')) = '' THEN 1 ELSE 0 END) AS empty_customer_city,
    SUM(CASE WHEN TRIM(REPLACE(customer_state, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_customer_state
FROM raw.customers
WHERE batch_id = @batch_id;


-- 6. Duplicate customer_id
SELECT customer_id, COUNT(*) AS cnt
FROM raw.customers
WHERE batch_id = @batch_id
GROUP BY customer_id
HAVING COUNT(*) > 1;


-- 7. Duplicate customer_unique_id
SELECT customer_unique_id, COUNT(*) AS cnt
FROM raw.customers
WHERE batch_id = @batch_id
GROUP BY customer_unique_id
HAVING COUNT(*) > 1
ORDER BY cnt DESC;


-- 8. customer_id vs customer_unique_id: returning customers
SELECT
    COUNT(*)                                                         AS total_rows,
    COUNT(DISTINCT customer_id)                                      AS distinct_customer_id,
    COUNT(DISTINCT customer_unique_id)                               AS distinct_customer_unique_id,
    COUNT(DISTINCT customer_id) - COUNT(DISTINCT customer_unique_id) AS returning_customers
FROM raw.customers
WHERE batch_id = @batch_id;


-- 9. Distribution by State
SELECT
    customer_state,
    COUNT(*)                                           AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS pct
FROM raw.customers
WHERE batch_id = @batch_id
GROUP BY customer_state
ORDER BY cnt DESC;


-- 10. Top 10 Cities by Customer Count
SELECT TOP 10
    customer_city,
    customer_state,
    COUNT(*) AS cnt
FROM raw.customers
WHERE batch_id = @batch_id
GROUP BY customer_city, customer_state
ORDER BY cnt DESC;


-- 11. Outliers: Cities with Inconsistent State Assignment
SELECT
    customer_city,
    COUNT(DISTINCT customer_state)                                           AS distinct_states,
    STRING_AGG(customer_state, ', ') WITHIN GROUP (ORDER BY customer_state)  AS states
FROM (
    SELECT DISTINCT customer_city, customer_state
    FROM raw.customers
    WHERE batch_id = @batch_id
) AS deduped
GROUP BY customer_city
HAVING COUNT(DISTINCT customer_state) > 1
ORDER BY distinct_states DESC;


-- 12. Referential Integrity: customers not referenced in orders
SELECT COUNT(*) AS customers_without_orders
FROM raw.customers c
WHERE c.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.orders o WHERE o.customer_id = c.customer_id
);
