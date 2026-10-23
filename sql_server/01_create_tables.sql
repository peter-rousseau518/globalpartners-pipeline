/* =====================================================================
   GlobalPartners - SQL Server source database
   01_create_tables.sql
   Creates:
     stg.*  : raw landing tables, every column text, loaded 1:1 from CSV
     dbo.*  : typed source tables the Glue ingest job reads over JDBC
   Run once against the RDS for SQL Server instance (as the master user).
   ===================================================================== */

IF DB_ID('GlobalPartners') IS NULL
    CREATE DATABASE GlobalPartners;
GO
USE GlobalPartners;
GO

IF SCHEMA_ID('stg') IS NULL EXEC('CREATE SCHEMA stg');
GO

/* ---------- Staging (raw text, exactly as in the CSV) ---------- */
DROP TABLE IF EXISTS stg.order_items;
CREATE TABLE stg.order_items (
    app_name            NVARCHAR(200),
    restaurant_id       NVARCHAR(50),
    creation_time_utc   NVARCHAR(50),
    order_id            NVARCHAR(50),
    user_id             NVARCHAR(50),
    printed_card_number NVARCHAR(50),
    is_loyalty          NVARCHAR(10),
    currency            NVARCHAR(10),
    lineitem_id         NVARCHAR(50),
    item_category       NVARCHAR(400),
    item_name           NVARCHAR(400),
    item_price          NVARCHAR(50),
    item_quantity       NVARCHAR(50)
);

DROP TABLE IF EXISTS stg.order_item_options;
CREATE TABLE stg.order_item_options (
    order_id          NVARCHAR(50),
    lineitem_id       NVARCHAR(50),
    option_group_name NVARCHAR(400),
    option_name       NVARCHAR(400),
    option_price      NVARCHAR(50),
    option_quantity   NVARCHAR(50)
);

DROP TABLE IF EXISTS stg.date_dim;
CREATE TABLE stg.date_dim (
    date_key     NVARCHAR(20),
    [year]       NVARCHAR(10),
    [month]      NVARCHAR(10),
    [week]       NVARCHAR(10),
    day_of_week  NVARCHAR(20),
    is_weekend   NVARCHAR(10),
    is_holiday   NVARCHAR(10),
    holiday_name NVARCHAR(100)
);
GO

/* ---------- Typed source tables (what Glue extracts) ---------- */
DROP TABLE IF EXISTS dbo.order_items;
CREATE TABLE dbo.order_items (
    row_id              BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    app_name            NVARCHAR(100)  NOT NULL,
    restaurant_id       VARCHAR(24)    NOT NULL,
    creation_time_utc   DATETIME2(3)   NOT NULL,
    order_id            VARCHAR(24)    NOT NULL,
    user_id             VARCHAR(24)    NULL,      -- NULL = guest order
    printed_card_number VARCHAR(20)    NULL,
    is_loyalty          BIT            NOT NULL,
    currency            CHAR(3)        NOT NULL,
    lineitem_id         VARCHAR(24)    NULL,      -- 1 malformed source row has none
    item_category       NVARCHAR(200)  NULL,
    item_name           NVARCHAR(200)  NULL,
    item_price          DECIMAL(10,2)  NOT NULL,
    item_quantity       INT            NOT NULL,
    loaded_at_utc       DATETIME2(3)   NOT NULL DEFAULT SYSUTCDATETIME()
);
-- Incremental extracts filter on creation_time_utc
CREATE INDEX ix_order_items_created ON dbo.order_items (creation_time_utc);
CREATE INDEX ix_order_items_order   ON dbo.order_items (order_id, lineitem_id);

DROP TABLE IF EXISTS dbo.order_item_options;
CREATE TABLE dbo.order_item_options (
    row_id            BIGINT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    order_id          VARCHAR(24)   NOT NULL,
    lineitem_id       VARCHAR(24)   NOT NULL,
    option_group_name NVARCHAR(200) NOT NULL,
    option_name       NVARCHAR(200) NOT NULL,
    option_price      DECIMAL(10,2) NOT NULL,
    option_quantity   INT           NOT NULL,
    loaded_at_utc     DATETIME2(3)  NOT NULL DEFAULT SYSUTCDATETIME()
);
CREATE INDEX ix_options_line ON dbo.order_item_options (order_id, lineitem_id);

DROP TABLE IF EXISTS dbo.date_dim;
CREATE TABLE dbo.date_dim (
    date_key     DATE         NOT NULL PRIMARY KEY,
    [year]       SMALLINT     NOT NULL,
    [month]      TINYINT      NOT NULL,   -- source holds month number, not name
    [week]       TINYINT      NOT NULL,
    day_of_week  VARCHAR(10)  NOT NULL,
    is_weekend   BIT          NOT NULL,
    is_holiday   BIT          NOT NULL,
    holiday_name NVARCHAR(100) NULL
);
GO
