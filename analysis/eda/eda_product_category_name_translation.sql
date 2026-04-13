USE OlistDWH;

-- EDA: raw.product_category_name_translation

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.product_category_name_translation
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'product_category_name'         AS column_name, LEN(product_category_name)         AS length FROM raw.product_category_name_translation WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_category_name_english' AS column_name, LEN(product_category_name_english) AS length FROM raw.product_category_name_translation WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Null Analysis
SELECT
    SUM(CASE WHEN product_category_name         IS NULL THEN 1 ELSE 0 END) AS null_category_name,
    SUM(CASE WHEN product_category_name_english IS NULL THEN 1 ELSE 0 END) AS null_category_name_english
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id;


-- 4. Empty String Analysis
SELECT
    SUM(CASE WHEN TRIM(product_category_name)         = '' THEN 1 ELSE 0 END) AS empty_category_name,
    SUM(CASE WHEN TRIM(product_category_name_english) = '' THEN 1 ELSE 0 END) AS empty_category_name_english
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id;


-- 5. Duplicate Portuguese Category Names
SELECT product_category_name, COUNT(*) AS duplicate_cnt
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id
GROUP BY product_category_name
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 6. Duplicate English Category Names
SELECT product_category_name_english, COUNT(*) AS duplicate_cnt
FROM raw.product_category_name_translation
WHERE batch_id = @batch_id
GROUP BY product_category_name_english
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 7. Referential Integrity: Portuguese names in products not covered by translation table
SELECT COUNT(*) AS products_categories_without_translation_cnt
FROM (
    SELECT DISTINCT product_category_name
    FROM raw.products
    WHERE batch_id = (SELECT TOP 1 batch_id FROM raw.products ORDER BY load_ts DESC)
      AND product_category_name IS NOT NULL
) AS pc
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.product_category_name_translation t
    WHERE t.batch_id = @batch_id
      AND t.product_category_name = pc.product_category_name
);


-- 7a. Sample: Portuguese names in products not covered by translation table
SELECT DISTINCT pc.product_category_name AS products_categories_without_translation
FROM (
    SELECT DISTINCT product_category_name
    FROM raw.products
    WHERE batch_id = (SELECT TOP 1 batch_id FROM raw.products ORDER BY load_ts DESC)
      AND product_category_name IS NOT NULL
) AS pc
WHERE NOT EXISTS (
    SELECT 1
    FROM raw.product_category_name_translation t
    WHERE t.batch_id = @batch_id
      AND t.product_category_name = pc.product_category_name
)
ORDER BY pc.product_category_name;
