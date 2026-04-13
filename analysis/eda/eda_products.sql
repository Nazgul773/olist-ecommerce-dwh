USE OlistDWH;

-- EDA: raw.products

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.products
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.products
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.products
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'product_id'                   AS column_name, LEN(product_id)                   AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_category_name'        AS column_name, LEN(product_category_name)        AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_name_lenght'          AS column_name, LEN(product_name_lenght)          AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_description_lenght'   AS column_name, LEN(product_description_lenght)   AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_photos_qty'           AS column_name, LEN(product_photos_qty)           AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_weight_g'             AS column_name, LEN(product_weight_g)             AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_length_cm'            AS column_name, LEN(product_length_cm)            AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_height_cm'            AS column_name, LEN(product_height_cm)            AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_width_cm'             AS column_name, LEN(product_width_cm)             AS length FROM raw.products WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'product_id'                   AS column_name, LEN(TRIM(REPLACE(product_id,            '"', ''))) AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_category_name'        AS column_name, LEN(TRIM(REPLACE(product_category_name, '"', ''))) AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_name_lenght'          AS column_name, LEN(TRIM(product_name_lenght))                     AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_description_lenght'   AS column_name, LEN(TRIM(product_description_lenght))              AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_photos_qty'           AS column_name, LEN(TRIM(product_photos_qty))                      AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_weight_g'             AS column_name, LEN(TRIM(product_weight_g))                        AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_length_cm'            AS column_name, LEN(TRIM(product_length_cm))                       AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_height_cm'            AS column_name, LEN(TRIM(product_height_cm))                       AS length FROM raw.products WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_width_cm'             AS column_name, LEN(TRIM(product_width_cm))                        AS length FROM raw.products WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN product_id IS NULL                 THEN 1 ELSE 0 END) AS null_product_id,
    SUM(CASE WHEN product_category_name IS NULL      THEN 1 ELSE 0 END) AS null_category_name,
    SUM(CASE WHEN product_name_lenght IS NULL        THEN 1 ELSE 0 END) AS null_name_lenght,
    SUM(CASE WHEN product_description_lenght IS NULL THEN 1 ELSE 0 END) AS null_description_lenght,
    SUM(CASE WHEN product_photos_qty IS NULL         THEN 1 ELSE 0 END) AS null_photos_qty,
    SUM(CASE WHEN product_weight_g IS NULL           THEN 1 ELSE 0 END) AS null_weight_g,
    SUM(CASE WHEN product_length_cm IS NULL          THEN 1 ELSE 0 END) AS null_length_cm,
    SUM(CASE WHEN product_height_cm IS NULL          THEN 1 ELSE 0 END) AS null_height_cm,
    SUM(CASE WHEN product_width_cm IS NULL           THEN 1 ELSE 0 END) AS null_width_cm
FROM raw.products
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(REPLACE(product_id, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_product_id,
    SUM(CASE WHEN TRIM(product_category_name)        = '' THEN 1 ELSE 0 END) AS empty_category_name
FROM raw.products
WHERE batch_id = @batch_id;


-- 6. Numeric Parse Failures
SELECT
    SUM(CASE WHEN product_name_lenght IS NOT NULL
             AND TRY_CAST(TRIM(product_name_lenght) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_name_lenght,
    SUM(CASE WHEN product_description_lenght IS NOT NULL
             AND TRY_CAST(TRIM(product_description_lenght) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_description_lenght,
    SUM(CASE WHEN product_photos_qty IS NOT NULL
             AND TRY_CAST(TRIM(product_photos_qty) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_photos_qty,
    SUM(CASE WHEN product_weight_g IS NOT NULL
             AND TRY_CAST(TRIM(product_weight_g) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_weight_g,
    SUM(CASE WHEN product_length_cm IS NOT NULL
             AND TRY_CAST(TRIM(product_length_cm) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_length_cm,
    SUM(CASE WHEN product_height_cm IS NOT NULL
             AND TRY_CAST(TRIM(product_height_cm) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_height_cm,
    SUM(CASE WHEN product_width_cm IS NOT NULL
             AND TRY_CAST(TRIM(product_width_cm) AS INT) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_width_cm
FROM raw.products
WHERE batch_id = @batch_id;


-- 7. Duplicate product_id
SELECT product_id, COUNT(*) AS duplicate_cnt
FROM raw.products
WHERE batch_id = @batch_id
GROUP BY product_id
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 8. Product Category Distribution
SELECT
    ISNULL(product_category_name, '(NULL)') AS product_category_name,
    COUNT(*)                                                   AS cnt,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2)         AS pct
FROM raw.products
WHERE batch_id = @batch_id
GROUP BY product_category_name
ORDER BY cnt DESC;


-- 9. Numeric Dimension Analysis (weight, size)
SELECT
    MIN(TRY_CAST(TRIM(product_weight_g)   AS INT)) AS min_weight_g,
    MAX(TRY_CAST(TRIM(product_weight_g)   AS INT)) AS max_weight_g,
    AVG(TRY_CAST(TRIM(product_weight_g)   AS INT)) AS avg_weight_g,
    MIN(TRY_CAST(TRIM(product_length_cm)  AS INT)) AS min_length_cm,
    MAX(TRY_CAST(TRIM(product_length_cm)  AS INT)) AS max_length_cm,
    MIN(TRY_CAST(TRIM(product_height_cm)  AS INT)) AS min_height_cm,
    MAX(TRY_CAST(TRIM(product_height_cm)  AS INT)) AS max_height_cm,
    MIN(TRY_CAST(TRIM(product_width_cm)   AS INT)) AS min_width_cm,
    MAX(TRY_CAST(TRIM(product_width_cm)   AS INT)) AS max_width_cm
FROM raw.products
WHERE batch_id = @batch_id;


-- 10. Outliers: Zero or Negative Dimensions
SELECT COUNT(*) AS zero_or_negative_dimensions_cnt
FROM raw.products
WHERE batch_id = @batch_id
  AND (
      TRY_CAST(TRIM(product_weight_g)  AS INT) <= 0
   OR TRY_CAST(TRIM(product_length_cm) AS INT) <= 0
   OR TRY_CAST(TRIM(product_height_cm) AS INT) <= 0
   OR TRY_CAST(TRIM(product_width_cm)  AS INT) <= 0
  );


-- 10a. Sample Rows: Zero or Negative Dimensions
SELECT
    product_id AS product_id_zero_or_negative_dimensions,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm
FROM raw.products
WHERE batch_id = @batch_id
  AND (
      TRY_CAST(TRIM(product_weight_g)  AS INT) <= 0
   OR TRY_CAST(TRIM(product_length_cm) AS INT) <= 0
   OR TRY_CAST(TRIM(product_height_cm) AS INT) <= 0
   OR TRY_CAST(TRIM(product_width_cm)  AS INT) <= 0
  );


-- 11. Photos per Product Distribution
SELECT
    TRY_CAST(TRIM(product_photos_qty) AS INT) AS photos_qty,
    COUNT(*)                                  AS cnt
FROM raw.products
WHERE batch_id = @batch_id
  AND TRY_CAST(TRIM(product_photos_qty) AS INT) IS NOT NULL
GROUP BY TRY_CAST(TRIM(product_photos_qty) AS INT)
ORDER BY photos_qty;


-- 12. Referential Integrity: products not referenced in order_items
SELECT COUNT(*) AS products_without_order_items
FROM raw.products p
WHERE p.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.order_items oi WHERE oi.product_id = p.product_id
);
