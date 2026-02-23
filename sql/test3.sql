DROP TABLE IF EXISTS spapi_app_user.FinancialTransactions;

CREATE TABLE spapi_app_user.FinancialTransactions (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,

    TransactionId NVARCHAR(255) NULL,
    PostedDate DATETIME NULL,
    TransactionType NVARCHAR(100) NULL,
    TransactionStatus NVARCHAR(100) NULL,
    AmazonOrderId NVARCHAR(50) NULL,
    SKU NVARCHAR(100) NULL,
    ASIN NVARCHAR(50) NULL,
    SSKU NVARCHAR(100) NULL,
    Brand NVARCHAR(200) NULL,
    Category NVARCHAR(200) NULL,
    Currency NVARCHAR(10) NULL,

    SOLD FLOAT NULL,
    ShippingCharge FLOAT NULL,
    TotalPromotions FLOAT NULL,
    SalesProceed FLOAT NULL,
    Fee FLOAT NULL,
    FBAFees FLOAT NULL,
    ShippingChargeback FLOAT NULL,
    TotalAmazonFees FLOAT NULL,
    VAT FLOAT NULL,
    R_VAT FLOAT NULL,
    FeePercent FLOAT NULL,
    COG FLOAT NULL,
    NetProfit FLOAT NULL,

    CreatedAt DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET(),
    UpdatedAt DATETIMEOFFSET DEFAULT SYSDATETIMEOFFSET()
);
