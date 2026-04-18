USE OlistDWH;
GO

CREATE TABLE raw.[customers] (
    [row_id]                    INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                  UNIQUEIDENTIFIER    NOT NULL,
    [customer_id]               NVARCHAR(255),
    [customer_unique_id]        NVARCHAR(255),
    [customer_zip_code_prefix]  NVARCHAR(255),
    [customer_city]             NVARCHAR(255),
    [customer_state]            NVARCHAR(255),
    [load_ts]                   DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                 NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_customers PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_customers_batch_id ON raw.customers (batch_id);
GO

CREATE TABLE raw.[orders] (
    [row_id]                           INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                         UNIQUEIDENTIFIER    NOT NULL,
    [order_id]                         NVARCHAR(255),
    [customer_id]                      NVARCHAR(255),
    [order_status]                     NVARCHAR(255),
    [order_purchase_timestamp]         NVARCHAR(255),
    [order_approved_at]                NVARCHAR(255),
    [order_delivered_carrier_date]     NVARCHAR(255),
    [order_delivered_customer_date]    NVARCHAR(255),
    [order_estimated_delivery_date]    NVARCHAR(255),
    [load_ts]                          DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                        NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_orders PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_orders_batch_id ON raw.orders (batch_id);
GO

CREATE TABLE raw.[order_items] (
    [row_id]               INT IDENTITY(1,1)   NOT NULL,
    [batch_id]             UNIQUEIDENTIFIER    NOT NULL,
    [order_id]             NVARCHAR(255),
    [order_item_id]        NVARCHAR(255),
    [product_id]           NVARCHAR(255),
    [seller_id]            NVARCHAR(255),
    [shipping_limit_date]  NVARCHAR(255),
    [price]                NVARCHAR(255),
    [freight_value]        NVARCHAR(255),
    [load_ts]              DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]            NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_order_items PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_order_items_batch_id ON raw.order_items (batch_id);
GO

CREATE TABLE raw.[geolocation] (
    [row_id]                      INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                    UNIQUEIDENTIFIER    NOT NULL,
    [geolocation_zip_code_prefix] NVARCHAR(255),
    [geolocation_lat]             NVARCHAR(255),
    [geolocation_lng]             NVARCHAR(255),
    [geolocation_city]            NVARCHAR(255),
    [geolocation_state]           NVARCHAR(255),
    [load_ts]                     DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                   NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_geolocation PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_geolocation_batch_id ON raw.geolocation (batch_id);
GO

CREATE TABLE raw.[order_payments] (
    [row_id]               INT IDENTITY(1,1)   NOT NULL,
    [batch_id]             UNIQUEIDENTIFIER    NOT NULL,
    [order_id]             NVARCHAR(255),
    [payment_sequential]   NVARCHAR(255),
    [payment_type]         NVARCHAR(255),
    [payment_installments] NVARCHAR(255),
    [payment_value]        NVARCHAR(255),
    [load_ts]              DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]            NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_order_payments PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_order_payments_batch_id ON raw.order_payments (batch_id);
GO

CREATE TABLE raw.[order_reviews] (
    [row_id]                   INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                 UNIQUEIDENTIFIER    NOT NULL,
    [review_id]                NVARCHAR(255),
    [order_id]                 NVARCHAR(255),
    [review_score]             NVARCHAR(255),
    [review_comment_title]     NVARCHAR(255),
    [review_comment_message]   NVARCHAR(MAX),
    [review_creation_date]     NVARCHAR(255),
    [review_answer_timestamp]  NVARCHAR(255),
    [load_ts]                  DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_order_reviews PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_order_reviews_batch_id ON raw.order_reviews (batch_id);
GO

CREATE TABLE raw.[products] (
    [row_id]                      INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                    UNIQUEIDENTIFIER    NOT NULL,
    [product_id]                  NVARCHAR(255),
    [product_category_name]       NVARCHAR(255),
    [product_name_lenght]         NVARCHAR(255),
    [product_description_lenght]  NVARCHAR(255),
    [product_photos_qty]          NVARCHAR(255),
    [product_weight_g]            NVARCHAR(255),
    [product_length_cm]           NVARCHAR(255),
    [product_height_cm]           NVARCHAR(255),
    [product_width_cm]            NVARCHAR(255),
    [load_ts]                     DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                   NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_products PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_products_batch_id ON raw.products (batch_id);
GO

CREATE TABLE raw.[sellers] (
    [row_id]                 INT IDENTITY(1,1)   NOT NULL,
    [batch_id]               UNIQUEIDENTIFIER    NOT NULL,
    [seller_id]              NVARCHAR(255),
    [seller_zip_code_prefix] NVARCHAR(255),
    [seller_city]            NVARCHAR(255),
    [seller_state]           NVARCHAR(255),
    [load_ts]                DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]              NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_sellers PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_sellers_batch_id ON raw.sellers (batch_id);
GO

CREATE TABLE raw.[product_category_name_translation] (
    [row_id]                         INT IDENTITY(1,1)   NOT NULL,
    [batch_id]                       UNIQUEIDENTIFIER    NOT NULL,
    [product_category_name]          NVARCHAR(255),
    [product_category_name_english]  NVARCHAR(255),
    [load_ts]                        DATETIME2(3)        NOT NULL DEFAULT SYSUTCDATETIME(),
    [file_name]                      NVARCHAR(255)       NOT NULL,
    CONSTRAINT PK_raw_product_category PRIMARY KEY (row_id)
);
GO
CREATE INDEX IX_product_category_name_translation_batch_id ON raw.product_category_name_translation (batch_id);
GO
