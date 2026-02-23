CREATE TABLE spapi_app_user.FinancialTransactions (
    TransactionId NVARCHAR(100) NOT NULL PRIMARY KEY,
    PostedDate DATETIME NULL,
    TransactionType NVARCHAR(100) NULL,
    TransactionStatus NVARCHAR(100) NULL,
    AmazonOrderId NVARCHAR(100) NULL,
    SKU NVARCHAR(100) NULL,
    ASIN NVARCHAR(50) NULL,
    SSKU NVARCHAR(100) NULL,
    Brand NVARCHAR(200) NULL,
    Category NVARCHAR(200) NULL,
    Currency NVARCHAR(10) NULL,

    SOLD DECIMAL(18, 4) NULL,
    ShippingCharge DECIMAL(18, 4) NULL,
    TotalPromotions DECIMAL(18, 4) NULL,
    SalesProceed DECIMAL(18, 4) NULL,

    Fee DECIMAL(18, 4) NULL,
    FBAFees DECIMAL(18, 4) NULL,
    ShippingChargeback DECIMAL(18, 4) NULL,
    TotalAmazonFees DECIMAL(18, 4) NULL,

    VAT DECIMAL(18, 4) NULL,
    R_VAT DECIMAL(18, 4) NULL,
    FeePercent DECIMAL(18, 4) NULL,

    COG DECIMAL(18, 4) NULL,
    NetProfit DECIMAL(18, 4) NULL,

    CreatedAt DATETIME NOT NULL,
    UpdatedAt DATETIME NOT NULL
);