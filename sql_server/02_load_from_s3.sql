/* =====================================================================
   GlobalPartners - SQL Server source database
   02_load_from_s3.sql
   Loads the three seed CSVs from S3 into RDS for SQL Server.
   Prereqs:
     - 01_create_tables.sql has run
     - CSVs are in s3://gp-seed-<ACCOUNT_ID>/sqlserver_seed/
     - An IAM role with read access to that bucket is attached to the
       instance with feature name S3_INTEGRATION
   Run STEP A, wait until all three tasks show SUCCESS, then run STEP B.
   ===================================================================== */
USE GlobalPartners;
GO

/* ---------------- STEP A: copy CSVs from S3 to D:\S3\seed\ ---------------- */
DECLARE @bucket NVARCHAR(100) = N'gp-seed-<ACCOUNT_ID>';   -- <== edit
DECLARE @prefix NVARCHAR(100) = N'sqlserver_seed/';
DECLARE @arn    NVARCHAR(400);

SET @arn = N'arn:aws:s3:::' + @bucket + N'/' + @prefix + N'order_items.csv';
EXEC msdb.dbo.rds_download_from_s3 @s3_arn_of_file = @arn,
     @rds_file_path = N'D:\S3\seed\order_items.csv', @overwrite_file = 1;

SET @arn = N'arn:aws:s3:::' + @bucket + N'/' + @prefix + N'order_item_options.csv';
EXEC msdb.dbo.rds_download_from_s3 @s3_arn_of_file = @arn,
     @rds_file_path = N'D:\S3\seed\order_item_options.csv', @overwrite_file = 1;

SET @arn = N'arn:aws:s3:::' + @bucket + N'/' + @prefix + N'date_dim.csv';
EXEC msdb.dbo.rds_download_from_s3 @s3_arn_of_file = @arn,
     @rds_file_path = N'D:\S3\seed\date_dim.csv', @overwrite_file = 1;
GO

-- Check progress (tasks can take a few minutes to start; re-run until SUCCESS)
SELECT task_id, task_type, lifecycle, created_at, last_updated, task_info
FROM msdb.dbo.rds_fn_task_status(NULL, 0)
ORDER BY task_id DESC;
GO

/* ---------------- STEP B: bulk load raw, then cast into typed tables ---------------- */
TRUNCATE TABLE stg.order_items;
TRUNCATE TABLE stg.order_item_options;
TRUNCATE TABLE stg.date_dim;

-- FORMAT='CSV' honors quoted fields: commas in item names and the
-- 12 item names that contain a line break inside quotes.
BULK INSERT stg.order_items
FROM 'D:\S3\seed\order_items.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDQUOTE = '"', FIELDTERMINATOR = ',',
      ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

BULK INSERT stg.order_item_options
FROM 'D:\S3\seed\order_item_options.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDQUOTE = '"', FIELDTERMINATOR = ',',
      ROWTERMINATOR = '0x0a', CODEPAGE = '65001', TABLOCK);

-- date_dim.csv uses Windows (CRLF) line endings
BULK INSERT stg.date_dim
FROM 'D:\S3\seed\date_dim.csv'
WITH (FORMAT = 'CSV', FIRSTROW = 2, FIELDQUOTE = '"', FIELDTERMINATOR = ',',
      ROWTERMINATOR = '0x0d0a', CODEPAGE = '65001', TABLOCK);
GO

TRUNCATE TABLE dbo.order_items;
TRUNCATE TABLE dbo.order_item_options;
TRUNCATE TABLE dbo.date_dim;

-- Timestamps arrive as ISO 8601 with 'Z', with or without milliseconds
INSERT INTO dbo.order_items
      (app_name, restaurant_id, creation_time_utc, order_id, user_id, printed_card_number,
       is_loyalty, currency, lineitem_id, item_category, item_name, item_price, item_quantity)
SELECT app_name,
       restaurant_id,
       CONVERT(DATETIME2(3), CONVERT(DATETIMEOFFSET(3), creation_time_utc, 127)),
       order_id,
       NULLIF(user_id, ''),
       NULLIF(printed_card_number, ''),
       CASE UPPER(is_loyalty) WHEN 'TRUE' THEN 1 ELSE 0 END,
       currency,
       NULLIF(lineitem_id, ''),
       NULLIF(item_category, ''),
       NULLIF(REPLACE(item_name, CHAR(10), ' '), ''),
       CAST(item_price AS DECIMAL(10,2)),
       CAST(item_quantity AS INT)
FROM stg.order_items;

INSERT INTO dbo.order_item_options
      (order_id, lineitem_id, option_group_name, option_name, option_price, option_quantity)
SELECT order_id, lineitem_id, option_group_name, option_name,
       CAST(option_price AS DECIMAL(10,2)), CAST(option_quantity AS INT)
FROM stg.order_item_options;

-- date_key is dd-mm-yyyy (style 105)
INSERT INTO dbo.date_dim
      (date_key, [year], [month], [week], day_of_week, is_weekend, is_holiday, holiday_name)
SELECT CONVERT(DATE, date_key, 105),
       CAST([year] AS SMALLINT), CAST([month] AS TINYINT), CAST([week] AS TINYINT),
       day_of_week,
       CASE UPPER(is_weekend) WHEN 'TRUE' THEN 1 ELSE 0 END,
       CASE UPPER(is_holiday) WHEN 'TRUE' THEN 1 ELSE 0 END,
       NULLIF(holiday_name, '')
FROM stg.date_dim;
GO

/* ---------------- STEP C: validate against the profiled counts ---------------- */
SELECT 'order_items'        AS tbl, COUNT(*) AS loaded, 203519 AS expected FROM dbo.order_items
UNION ALL
SELECT 'order_item_options',         COUNT(*),           193017             FROM dbo.order_item_options
UNION ALL
SELECT 'date_dim',                   COUNT(*),           365                FROM dbo.date_dim;

SELECT MIN(creation_time_utc) AS first_order_utc,   -- expect 2020-04-21
       MAX(creation_time_utc) AS last_order_utc,    -- expect 2024-02-21
       COUNT(DISTINCT order_id)      AS orders,     -- expect 131,328
       COUNT(DISTINCT restaurant_id) AS restaurants -- expect 28
FROM dbo.order_items;
GO
