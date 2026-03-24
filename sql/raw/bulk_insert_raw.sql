USE OlistDWH;
GO

BULK INSERT raw.[customers]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_customers_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[orders]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_orders_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[order_items]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_order_items_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[order_payments]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_order_payments_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[order_reviews]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_order_reviews_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[sellers]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_sellers_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[products]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_products_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[geolocation]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\olist_geolocation_dataset.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO

BULK INSERT raw.[product_category_name_translation]
FROM 'D:\Code\Datasets\brazilian_ecommerce_dataset_olist\product_category_name_translation.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = ',',
    ROWTERMINATOR = '0x0a',
    CODEPAGE = '65001'
);
GO
