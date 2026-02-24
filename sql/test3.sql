------------------------------------------------------------
-- DROP TABLE (safe)
------------------------------------------------------------
IF OBJECT_ID('spapi_app_user.FBABuyBoxAnalysis', 'U') IS NOT NULL
    DROP TABLE spapi_app_user.FBABuyBoxAnalysis;


------------------------------------------------------------
-- RECREATE TABLE (DATETIME ONLY — NO OFFSET)
------------------------------------------------------------
CREATE TABLE spapi_app_user.FBABuyBoxAnalysis (
    id INT IDENTITY(1,1) PRIMARY KEY,

    asin NVARCHAR(50) NOT NULL,
    product_name NVARCHAR(500),

    winner_seller_id NVARCHAR(50),
    winner_store_name NVARCHAR(500),
    winner_price FLOAT,
    winner_total_price FLOAT,

    my_price FLOAT,
    my_shipping FLOAT,
    my_total FLOAT,
    my_is_buybox BIT,

    summary_buybox_price FLOAT,
    lowest_price_amazon FLOAT,
    lowest_price_merchant FLOAT,

    analysis_timestamp DATETIME,   -- UTC+4 naive
    created_at DATETIME,           -- UTC+4 naive
    updated_at DATETIME            -- UTC+4 naive
);


------------------------------------------------------------
-- RECOMMENDED INDEXES
------------------------------------------------------------
CREATE INDEX IX_FBABuyBoxAnalysis_ASIN
ON spapi_app_user.FBABuyBoxAnalysis (asin);
