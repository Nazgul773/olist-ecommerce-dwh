USE OlistDWH;
GO

-- DDL: cleansed schema tables

CREATE TABLE cleansed.customers (
    customer_id              NVARCHAR(32)  NOT NULL,
    customer_unique_id       NVARCHAR(32)  NOT NULL,
    customer_zip_code_prefix CHAR(5)       NOT NULL,
    customer_city            NVARCHAR(100) NOT NULL,
    customer_state           CHAR(2)       NOT NULL,
    row_hash                 BINARY(32)    NOT NULL,
    is_deleted               BIT           NOT NULL DEFAULT 0,
    deleted_at               DATETIME2(3)  NULL,
    updated_at               DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_customers PRIMARY KEY (customer_id)
);
GO

CREATE TABLE cleansed.geolocation (
    geolocation_zip_code_prefix  CHAR(5)        NOT NULL,
    geolocation_lat              DECIMAL(10,7)  NOT NULL,
    geolocation_lng              DECIMAL(10,7)  NOT NULL,
    geolocation_city             NVARCHAR(100)  NOT NULL,
    geolocation_state            CHAR(2)        NOT NULL,
    row_hash                     BINARY(32)     NOT NULL,
    is_deleted                   BIT            NOT NULL DEFAULT 0,
    deleted_at                   DATETIME2(3)   NULL,
    updated_at                   DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_geolocation PRIMARY KEY (geolocation_zip_code_prefix)
);
GO

CREATE TABLE cleansed.order_items (
    order_id             NVARCHAR(32)   NOT NULL,
    order_item_id        NVARCHAR(25)   NOT NULL,
    product_id           NVARCHAR(32)   NOT NULL,
    seller_id            NVARCHAR(32)   NOT NULL,
    shipping_limit_date  DATETIME2(0)   NOT NULL,
    price                DECIMAL(10,2)  NOT NULL,
    freight_value        DECIMAL(10,2)  NOT NULL,
    row_hash             BINARY(32)     NOT NULL,
    is_deleted           BIT            NOT NULL DEFAULT 0,
    deleted_at           DATETIME2(3)   NULL,
    updated_at           DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_order_items PRIMARY KEY (order_id, order_item_id)
);
GO

CREATE TABLE cleansed.order_payments (
    order_id              NVARCHAR(32)   NOT NULL,
    payment_sequential    INT            NOT NULL,
    payment_type          NVARCHAR(25)   NOT NULL,
    payment_installments  INT            NOT NULL,
    payment_value         DECIMAL(10,2)  NOT NULL,
    row_hash              BINARY(32)     NOT NULL,
    is_deleted            BIT            NOT NULL DEFAULT 0,
    deleted_at            DATETIME2(3)   NULL,
    updated_at            DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_order_payments PRIMARY KEY (order_id, payment_sequential)
);
GO

CREATE TABLE cleansed.order_reviews (
    review_id               NVARCHAR(32)   NOT NULL,
    order_id                NVARCHAR(32)   NOT NULL,
    review_score            TINYINT        NOT NULL,
    review_comment_title    NVARCHAR(255)  NULL,
    review_comment_message  NVARCHAR(MAX)  NULL,
    review_creation_date    DATETIME2(0)   NOT NULL,
    review_answer_timestamp DATETIME2(0)   NOT NULL,
    row_hash                BINARY(32)     NOT NULL,
    is_deleted              BIT            NOT NULL DEFAULT 0,
    deleted_at              DATETIME2(3)   NULL,
    updated_at              DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_order_reviews PRIMARY KEY (review_id)
);
GO

CREATE TABLE cleansed.orders (
    order_id                       NVARCHAR(32)  NOT NULL,
    customer_id                    NVARCHAR(32)  NOT NULL,
    order_status                   NVARCHAR(25)  NOT NULL,
    order_purchase_timestamp       DATETIME2(0)  NOT NULL,
    order_approved_at              DATETIME2(0)  NULL,
    order_delivered_carrier_date   DATETIME2(0)  NULL,
    order_delivered_customer_date  DATETIME2(0)  NULL,
    order_estimated_delivery_date  DATETIME2(0)  NOT NULL,
    row_hash                       BINARY(32)    NOT NULL,
    is_deleted                     BIT           NOT NULL DEFAULT 0,
    deleted_at                     DATETIME2(3)  NULL,
    updated_at                     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_orders PRIMARY KEY (order_id)
);
GO

CREATE TABLE cleansed.product_category_name_translation (
    product_category_name          NVARCHAR(100) NOT NULL,
    product_category_name_english  NVARCHAR(100) NOT NULL,
    row_hash                       BINARY(32)    NOT NULL,
    is_deleted                     BIT           NOT NULL DEFAULT 0,
    deleted_at                     DATETIME2(3)  NULL,
    updated_at                     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_product_category_name_translation PRIMARY KEY (product_category_name)
);
GO

CREATE TABLE cleansed.products (
    product_id                   NVARCHAR(32)   NOT NULL,
    product_category_name        NVARCHAR(100)  NULL,
    product_name_lenght          INT            NULL,
    product_description_lenght   INT            NULL,
    product_photos_qty           INT            NULL,
    product_weight_g             INT            NULL,
    product_length_cm            INT            NULL,
    product_height_cm            INT            NULL,
    product_width_cm             INT            NULL,
    row_hash                     BINARY(32)     NOT NULL,
    is_deleted                   BIT            NOT NULL DEFAULT 0,
    deleted_at                   DATETIME2(3)   NULL,
    updated_at                   DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_products PRIMARY KEY (product_id)
);
GO

CREATE TABLE cleansed.sellers (
    seller_id               NVARCHAR(32)  NOT NULL,
    seller_zip_code_prefix  CHAR(5)       NOT NULL,
    seller_city             NVARCHAR(100) NOT NULL,
    seller_state            CHAR(2)       NOT NULL,
    row_hash                BINARY(32)    NOT NULL,
    is_deleted              BIT           NOT NULL DEFAULT 0,
    deleted_at              DATETIME2(3)  NULL,
    updated_at              DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME(),
    CONSTRAINT PK_cleansed_sellers PRIMARY KEY (seller_id)
);
GO
