USE OlistDWH;
GO

-- Migration: V007
-- Description: Extend V005 — update pipeline_config file paths for sellers and
--              customers to point to pipe-delimited files. These files handle quoted CSV
--              fields containing commas and embedded newlines.
-- Applied: manually in SSMS

DECLARE @DatasetRoot NVARCHAR(500);
SELECT @DatasetRoot = LEFT(file_path, LEN(file_path) - LEN(file_name))
FROM orchestration.pipeline_config
WHERE table_name = 'geolocation' AND layer = 'RAW';

UPDATE orchestration.pipeline_config
SET file_path = @DatasetRoot + 'olist_sellers_dataset_pipe.csv',
    file_name = 'olist_sellers_dataset_pipe.csv'
WHERE table_name = 'sellers' AND layer = 'RAW';

UPDATE orchestration.pipeline_config
SET file_path = @DatasetRoot + 'olist_customers_dataset_pipe.csv',
    file_name = 'olist_customers_dataset_pipe.csv'
WHERE table_name = 'customers' AND layer = 'RAW';
GO
