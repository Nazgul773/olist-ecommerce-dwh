USE OlistDWH;
GO

-- Mart layer — Kimball star schema.
-- Fact tables use TRUNCATE + full reload; dimensions use MERGE (stable surrogates).
-- FK constraints are omitted on fact tables: they block TRUNCATE and add write overhead.
-- Referential integrity is enforced by ETL (unknown member -1 / 0 as fallback).

-- Populated once by mart.sp_load_dim_date. Covers 2016–2025 by default.
-- Unknown/missing date: date_key = 0 (sentinel row).
CREATE TABLE mart.dim_date (
    date_key        INT           NOT NULL,  -- YYYYMMDD surrogate (e.g. 20181123)
    full_date       DATE          NOT NULL,
    year            SMALLINT      NOT NULL,
    iso_year        SMALLINT      NOT NULL,  -- ISO 8601 year the week belongs to
    quarter         TINYINT       NOT NULL,  -- 1–4
    month           TINYINT       NOT NULL,  -- 1–12
    month_name      NVARCHAR(9)   NOT NULL,  -- 'January' … 'December'
    week_of_year    TINYINT       NOT NULL,  -- ISO week number
    day_of_month    TINYINT       NOT NULL,  -- 1–31
    day_of_week     TINYINT       NOT NULL,  -- 1=Sun, 7=Sat
    day_name        NVARCHAR(9)   NOT NULL,  -- 'Monday' … 'Sunday'
    is_weekend      BIT           NOT NULL,  -- 1 if Saturday or Sunday
    CONSTRAINT PK_mart_dim_date PRIMARY KEY (date_key)
);
GO

-- SCD Type 1. customer_id is order-level; customer_unique_id is the true entity
-- (enables repeat-buyer analysis). Lat/lng denormalised from cleansed.geolocation via zip.
-- Unknown member: customer_key = -1.
CREATE TABLE mart.dim_customer (
    customer_key             INT IDENTITY(1,1) NOT NULL,
    customer_id              NVARCHAR(32)      NOT NULL,
    customer_unique_id       NVARCHAR(32)      NOT NULL,
    customer_zip_code        CHAR(5)           NOT NULL,
    customer_city            NVARCHAR(100)     NOT NULL,
    customer_state           CHAR(2)           NOT NULL,
    customer_lat             DECIMAL(10,7)     NULL,  -- NULL when zip not in geolocation
    customer_lng             DECIMAL(10,7)     NULL,
    is_deleted               BIT               NOT NULL DEFAULT 0,
    row_hash                 BINARY(32)        NULL,
    updated_at               DATETIME2(3)      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_dim_customer PRIMARY KEY (customer_key)
);
GO
CREATE UNIQUE INDEX UX_mart_dim_customer_id ON mart.dim_customer (customer_id);
GO

-- SCD Type 1. Lat/lng denormalised from cleansed.geolocation via zip.
-- Unknown member: seller_key = -1.
CREATE TABLE mart.dim_seller (
    seller_key               INT IDENTITY(1,1) NOT NULL,
    seller_id                NVARCHAR(32)      NOT NULL,
    seller_zip_code          CHAR(5)           NOT NULL,
    seller_city              NVARCHAR(100)     NOT NULL,
    seller_state             CHAR(2)           NOT NULL,
    seller_lat               DECIMAL(10,7)     NULL,  -- NULL when zip not in geolocation
    seller_lng               DECIMAL(10,7)     NULL,
    is_deleted               BIT               NOT NULL DEFAULT 0,
    row_hash                 BINARY(32)        NULL,
    updated_at               DATETIME2(3)      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_dim_seller PRIMARY KEY (seller_key)
);
GO
CREATE UNIQUE INDEX UX_mart_dim_seller_id ON mart.dim_seller (seller_id);
GO

-- SCD Type 1.
-- Unknown member: product_key = -1.
CREATE TABLE mart.dim_product (
    product_key                    INT IDENTITY(1,1) NOT NULL,
    product_id                     NVARCHAR(32)      NOT NULL,
    product_category_name          NVARCHAR(100)     NULL,  -- source (PT)
    product_category_name_english  NVARCHAR(100)     NULL,  -- translated (EN)
    product_name_length            INT               NULL,
    product_description_length     INT               NULL,
    product_photos_qty             INT               NULL,
    product_weight_g               INT               NULL,
    product_length_cm              INT               NULL,
    product_height_cm              INT               NULL,
    product_width_cm               INT               NULL,
    is_deleted                     BIT               NOT NULL DEFAULT 0,
    row_hash                       BINARY(32)        NULL,
    updated_at                     DATETIME2(3)      NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_dim_product PRIMARY KEY (product_key)
);
GO
CREATE UNIQUE INDEX UX_mart_dim_product_id ON mart.dim_product (product_id);
GO

-- Static lookup seeded by mart.sp_load_dim_payment_type (idempotent).
-- No IDENTITY: explicit INT PK required for unknown member (payment_type_key = -1).
CREATE TABLE mart.dim_payment_type (
    payment_type_key    INT           NOT NULL,
    payment_type_name   NVARCHAR(25)  NOT NULL,
    CONSTRAINT PK_mart_dim_payment_type PRIMARY KEY (payment_type_key)
);
GO

-- Static lookup seeded by mart.sp_load_dim_order_status (idempotent).
-- No IDENTITY: explicit INT PK required for unknown member (order_status_key = -1).
CREATE TABLE mart.dim_order_status (
    order_status_key    INT           NOT NULL,
    status_name         NVARCHAR(25)  NOT NULL,
    status_category     NVARCHAR(25)  NOT NULL,  -- 'in_progress', 'completed', 'canceled', 'unknown'
    sort_order          TINYINT       NOT NULL,
    CONSTRAINT PK_mart_dim_order_status PRIMARY KEY (order_status_key)
);
GO

-- Grain: one row per order item (order_id + order_item_id).
-- Role-playing date dimension: four date FKs (purchase, estimated delivery,
-- carrier handoff, actual delivery).
CREATE TABLE mart.fact_sales (
    sales_key                    BIGINT IDENTITY(1,1) NOT NULL,
    purchase_date_key            INT           NOT NULL,  -- -> dim_date (purchase)
    estimated_delivery_date_key  INT           NOT NULL,  -- -> dim_date (SLA target)
    carrier_handoff_date_key     INT           NULL,      -- -> dim_date (NULL = not yet handed off)
    actual_delivery_date_key     INT           NULL,      -- -> dim_date (NULL = not yet delivered)
    customer_key                 INT           NOT NULL,  -- -1 = unknown
    seller_key                   INT           NOT NULL,  -- -1 = unknown
    product_key                  INT           NOT NULL,  -- -1 = unknown
    order_status_key             INT           NOT NULL,  -- -1 = unknown
    order_id                     NVARCHAR(32)  NOT NULL,  -- degenerate dimension
    order_item_id                INT           NOT NULL,  -- degenerate dimension
    price                        DECIMAL(10,2) NOT NULL,
    freight_value                DECIMAL(10,2) NOT NULL,
    total_value                  DECIMAL(10,2) NOT NULL,  -- price + freight_value
    purchase_to_delivery_days    INT           NULL,  -- NULL if not yet delivered
    delivery_vs_estimate_days    INT           NULL,  -- negative = early, positive = late
    purchase_to_approval_hours   INT           NULL,  -- NULL if not yet approved
    carrier_to_delivery_days     INT           NULL,  -- NULL if not yet delivered
    review_score                 TINYINT       NULL,  -- NULL if no review submitted
    mart_load_ts                 DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_fact_sales PRIMARY KEY (sales_key)
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_mart_fact_sales
ON mart.fact_sales (
    purchase_date_key, estimated_delivery_date_key,
    carrier_handoff_date_key, actual_delivery_date_key,
    customer_key, seller_key, product_key,
    order_status_key,
    order_id, order_item_id,
    price, freight_value, total_value,
    purchase_to_delivery_days, delivery_vs_estimate_days,
    purchase_to_approval_hours, carrier_to_delivery_days,
    review_score
);
GO

-- Grain: one row per payment record (order_id + payment_sequential).
-- An order can have multiple records (split payment types, e.g. voucher + credit card).
CREATE TABLE mart.fact_payments (
    payment_fact_key     BIGINT IDENTITY(1,1) NOT NULL,
    purchase_date_key    INT            NOT NULL,  -- -> dim_date
    customer_key         INT            NOT NULL,  -- -1 = unknown
    payment_type_key     INT            NOT NULL,  -- -1 = unknown
    order_id             NVARCHAR(32)   NOT NULL,  -- degenerate dimension
    payment_sequential   INT            NOT NULL,  -- position in multi-payment order
    payment_installments INT            NOT NULL,
    payment_value        DECIMAL(10,2)  NOT NULL,
    mart_load_ts         DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_mart_fact_payments PRIMARY KEY (payment_fact_key)
);
GO

CREATE NONCLUSTERED COLUMNSTORE INDEX NCCI_mart_fact_payments
ON mart.fact_payments (
    purchase_date_key,
    customer_key, payment_type_key,
    order_id, payment_sequential,
    payment_installments, payment_value
);
GO
