USE OlistDWH;
GO

DECLARE @base      NVARCHAR(500) = 'D:\Code\Datasets\olist_data\';
DECLARE @file_path NVARCHAR(500);

SET @file_path = @base + 'olist_customers_dataset.csv';

EXEC raw.sp_load_customers
    @file_path = @file_path,
    @file_name = 'olist_customers_dataset.csv';
