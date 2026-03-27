USE OlistDWH;
GO

-- ============================================================
-- DDL: cleansed.error_log
-- ============================================================
CREATE TABLE cleansed.error_log (
    [row_id]          INT IDENTITY(1,1)   NOT NULL,
    [batch_id]        UNIQUEIDENTIFIER    NOT NULL,
    [table_name]      NVARCHAR(100)       NOT NULL,
    [raw_key]         NVARCHAR(100)       NULL,
    [column_name]     NVARCHAR(100)       NOT NULL,
    [issue]           NVARCHAR(255)       NOT NULL,
    [raw_value]       NVARCHAR(MAX)       NULL,
    logged_at       DATETIME            NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_error_log PRIMARY KEY (row_id)
);
GO


-- ============================================================
-- DDL: cleansed schema tables
-- ============================================================

-- customers
-- ============================================================
CREATE TABLE cleansed.customers (
    customer_id              NVARCHAR(32)  NOT NULL,
    customer_unique_id       NVARCHAR(32)  NOT NULL,
    customer_zip_code_prefix CHAR(5)       NOT NULL,
    customer_city            NVARCHAR(100) NOT NULL,
    customer_state           CHAR(2)       NOT NULL,
    row_hash                 VARBINARY(32)  NOT NULL,
    updated_at               DATETIME       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_cleansed_customers PRIMARY KEY (customer_id)
);
GO

-- orders
-- ============================================================
CREATE TABLE cleansed.orders (
    order_id                        NVARCHAR(32)    NOT NULL,
    customer_id                     NVARCHAR(32)    NOT NULL,
    order_status                    NVARCHAR(25)    NOT NULL,
    order_purchase_timestamp        DATETIME        NOT NULL,
    order_approved_at               DATETIME        NULL,
    order_delivered_carrier_date    DATETIME        NULL,
    order_delivered_customer_date   DATETIME        NULL,
    order_estimated_delivery_date   DATETIME        NOT NULL,
    row_hash                        VARBINARY(32)  NOT NULL,
    updated_at                      DATETIME       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_cleansed_orders PRIMARY KEY (order_id)
);
GO

-- order_items
-- ============================================================
CREATE TABLE cleansed.order_items (
    order_id                        NVARCHAR(32)    NOT NULL,
    order_item_id                   NVARCHAR(25)    NOT NULL,
    product_id                      NVARCHAR(32)    NOT NULL,
    seller_id                       NVARCHAR(32)    NOT NULL,
    shipping_limit_date             DATETIME        NOT NULL,
    price                           DECIMAL(10,2)   NOT NULL,
    freight_value                   DECIMAL(10,2)   NOT NULL,
    row_hash                        VARBINARY(32)  NOT NULL,
    updated_at                      DATETIME       NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_cleansed_order_items PRIMARY KEY (order_id, order_item_id)
);
GO
