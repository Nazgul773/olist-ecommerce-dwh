USE OlistDWH;

-- EDA: raw.order_items

-- Analysis is based on the most recent batch_id to reflect the latest data state. Adjust as needed for historical analysis.
DECLARE @batch_id UNIQUEIDENTIFIER;
SELECT TOP 1 @batch_id = batch_id
FROM raw.order_items
ORDER BY load_ts DESC;


-- 0. Data Preview
SELECT TOP 10 *
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 1. Row Count
SELECT COUNT(*) AS total_rows
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 2. Min/Max Character Length per Column
SELECT column_name, MIN(length) AS min_length, MAX(length) AS max_length
FROM (
    SELECT 'order_id'            AS column_name, LEN(order_id)            AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_item_id'       AS column_name, LEN(order_item_id)       AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_id'          AS column_name, LEN(product_id)          AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_id'           AS column_name, LEN(seller_id)           AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'shipping_limit_date' AS column_name, LEN(shipping_limit_date) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'price'               AS column_name, LEN(price)               AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'freight_value'       AS column_name, LEN(freight_value)       AS length FROM raw.order_items WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 3. Min/Max Character Length per Column (quotes cleansed)
SELECT column_name, MIN(length) AS min_length_quotes_cleansed, MAX(length) AS max_length_quotes_cleansed
FROM (
    SELECT 'order_id'            AS column_name, LEN(TRIM(REPLACE(order_id,   '"', ''))) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'order_item_id'       AS column_name, LEN(TRIM(REPLACE(order_item_id,       '"', ''))) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'product_id'          AS column_name, LEN(TRIM(REPLACE(product_id,          '"', ''))) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'seller_id'           AS column_name, LEN(TRIM(REPLACE(seller_id,           '"', ''))) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'shipping_limit_date' AS column_name, LEN(TRIM(REPLACE(shipping_limit_date, '"', ''))) AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'price'               AS column_name, LEN(TRIM(price))                                 AS length FROM raw.order_items WHERE batch_id = @batch_id
    UNION ALL
    SELECT 'freight_value'       AS column_name, LEN(TRIM(freight_value))                         AS length FROM raw.order_items WHERE batch_id = @batch_id
) AS lengths
GROUP BY column_name
ORDER BY column_name;


-- 4. Null Analysis
SELECT
    SUM(CASE WHEN order_id IS NULL THEN 1 ELSE 0 END)            AS null_order_id,
    SUM(CASE WHEN order_item_id IS NULL THEN 1 ELSE 0 END)       AS null_order_item_id,
    SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END)          AS null_product_id,
    SUM(CASE WHEN seller_id IS NULL THEN 1 ELSE 0 END)           AS null_seller_id,
    SUM(CASE WHEN shipping_limit_date IS NULL THEN 1 ELSE 0 END) AS null_shipping_limit_date,
    SUM(CASE WHEN price IS NULL THEN 1 ELSE 0 END)               AS null_price,
    SUM(CASE WHEN freight_value IS NULL THEN 1 ELSE 0 END)       AS null_freight_value
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 5. Empty String Analysis (after cleansing)
SELECT
    SUM(CASE WHEN TRIM(REPLACE(order_id,   '"', '')) = '' THEN 1 ELSE 0 END) AS empty_order_id,
    SUM(CASE WHEN TRIM(order_item_id) = ''               THEN 1 ELSE 0 END)  AS empty_order_item_id,
    SUM(CASE WHEN TRIM(REPLACE(product_id, '"', '')) = '' THEN 1 ELSE 0 END) AS empty_product_id,
    SUM(CASE WHEN TRIM(REPLACE(seller_id,  '"', '')) = '' THEN 1 ELSE 0 END) AS empty_seller_id
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 6. Date Parse Failures
SELECT
    SUM(CASE WHEN shipping_limit_date IS NOT NULL
             AND TRY_CONVERT(DATETIME2(0), TRIM(shipping_limit_date), 120) IS NULL
             THEN 1 ELSE 0 END) AS unparseable_shipping_limit_date
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 7. Decimal Parse Failures
SELECT
    SUM(CASE WHEN price IS NOT NULL         AND TRY_CONVERT(DECIMAL(10,2), TRIM(price))         IS NULL THEN 1 ELSE 0 END) AS unparseable_price,
    SUM(CASE WHEN freight_value IS NOT NULL AND TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value)) IS NULL THEN 1 ELSE 0 END) AS unparseable_freight_value
FROM raw.order_items
WHERE batch_id = @batch_id;


-- 8. Duplicate Check (composite key: order_id + order_item_id)
SELECT order_id, order_item_id, COUNT(*) AS duplicate_cnt
FROM raw.order_items
WHERE batch_id = @batch_id
GROUP BY order_id, order_item_id
HAVING COUNT(*) > 1
ORDER BY duplicate_cnt DESC;


-- 9. Items per Order Distribution
SELECT
    TRY_CAST(order_item_id AS INT) AS item_position,
    COUNT(*)                        AS orders_with_this_position
FROM raw.order_items
WHERE batch_id = @batch_id
GROUP BY TRY_CAST(order_item_id AS INT)
ORDER BY item_position ASC;


-- 10. Multi-Item Orders
SELECT
    COUNT(DISTINCT order_id)                                                          AS total_orders,
    SUM(CASE WHEN item_count > 1 THEN 1 ELSE 0 END)                                  AS multi_item_orders,
    ROUND(SUM(CASE WHEN item_count > 1 THEN 1.0 ELSE 0 END) * 100.0 / COUNT(*), 2)   AS pct_multi_item
FROM (
    SELECT order_id, COUNT(*) AS item_count
    FROM raw.order_items
    WHERE batch_id = @batch_id
    GROUP BY order_id
) AS order_counts;


-- 11. Price Analysis
SELECT
    MIN(price_val)        AS min_price,
    MAX(price_val)        AS max_price,
    AVG(price_val)        AS avg_price,
    MAX(median_price_val) AS median_price
FROM (
    SELECT
        price_val,
        ROUND(PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY price_val) OVER (), 2) AS median_price_val
    FROM (
        SELECT TRY_CONVERT(DECIMAL(10,2), TRIM(price)) AS price_val
        FROM raw.order_items
        WHERE batch_id = @batch_id
          AND TRY_CONVERT(DECIMAL(10,2), TRIM(price)) IS NOT NULL
    ) AS converted
) AS computed;


-- 12. Freight Value Analysis
SELECT
    MIN(TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value))) AS min_freight,
    MAX(TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value))) AS max_freight,
    AVG(TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value))) AS avg_freight
FROM raw.order_items
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value)) IS NOT NULL;


-- 13. Outliers: Zero or Negative Prices
SELECT COUNT(*) AS zero_or_negative_price
FROM raw.order_items
WHERE batch_id = @batch_id
  AND TRY_CONVERT(DECIMAL(10,2), TRIM(price)) <= 0;


-- 14. Outliers: Extremely High Prices (top 10)
SELECT TOP 10
    order_id,
    product_id,
    order_item_id,
    TRY_CONVERT(DECIMAL(10,2), TRIM(price))         AS price,
    TRY_CONVERT(DECIMAL(10,2), TRIM(freight_value)) AS freight_value
FROM raw.order_items
WHERE batch_id = @batch_id
ORDER BY TRY_CONVERT(DECIMAL(10,2), TRIM(price)) DESC;


-- 15. Top 10 Sellers by Volume
SELECT TOP 10
    seller_id,
    COUNT(*)                                               AS total_items_sold,
    ROUND(SUM(TRY_CONVERT(DECIMAL(10,2), TRIM(price))), 2) AS total_revenue
FROM raw.order_items
WHERE batch_id = @batch_id
GROUP BY seller_id
ORDER BY total_items_sold DESC;


-- 16. Referential Integrity: order_items without a matching order
SELECT COUNT(*) AS items_without_order
FROM raw.order_items oi
WHERE oi.batch_id = @batch_id
  AND NOT EXISTS (
    SELECT 1 FROM raw.orders o WHERE o.order_id = oi.order_id
);
