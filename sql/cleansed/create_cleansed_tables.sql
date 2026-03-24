USE OlistDWH;
GO

-- ============================================================
-- DDL: cleansed.error_log
-- ============================================================
CREATE TABLE cleansed.error_log (
    log_id          INT IDENTITY(1,1)   NOT NULL,
    table_name      NVARCHAR(100)       NOT NULL,
    raw_key         NVARCHAR(100)       NULL,
    column_name     NVARCHAR(100)       NOT NULL,
    issue           NVARCHAR(255)       NOT NULL,
    raw_value       NVARCHAR(MAX)       NULL,
    logged_at       DATETIME            NOT NULL DEFAULT GETDATE(),
    CONSTRAINT PK_error_log PRIMARY KEY (log_id)
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
    CONSTRAINT PK_cleansed_customers PRIMARY KEY (customer_id)
);
GO

-- orders
-- ============================================================

