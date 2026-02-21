DROP TABLE IF EXISTS spapi_app_user.FBABuyBoxAnalysis;

CREATE TABLE spapi_app_user.FBABuyBoxAnalysis (
    id BIGINT IDENTITY(1,1) PRIMARY KEY,

    asin NVARCHAR(20) NOT NULL,
    product_name NVARCHAR(500) NULL,

    winner_seller_id NVARCHAR(50) NULL,
    winner_store_name NVARCHAR(200) NULL,
    winner_price DECIMAL(18,4) NULL,
    winner_total_price DECIMAL(18,4) NULL,

    my_price DECIMAL(18,4) NULL,
    my_shipping DECIMAL(18,4) NULL,
    my_total DECIMAL(18,4) NULL,
    my_is_buybox BIT NULL,   -- ✔ BOOLEAN FLAG (BIT)

    summary_buybox_price DECIMAL(18,4) NULL,
    lowest_price_amazon DECIMAL(18,4) NULL,
    lowest_price_merchant DECIMAL(18,4) NULL,

    analysis_timestamp DATETIMEOFFSET NOT NULL,

    created_at DATETIMEOFFSET NOT NULL DEFAULT (DATEADD(HOUR,4,SYSDATETIMEOFFSET())),
    updated_at DATETIMEOFFSET NOT NULL DEFAULT (DATEADD(HOUR,4,SYSDATETIMEOFFSET()))
);
