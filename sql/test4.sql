DROP TABLE IF EXISTS spapi_app_user.FinancialTransactions;
GO

CREATE TABLE spapi_app_user.FinancialTransactions (
    Id BIGINT IDENTITY(1,1) PRIMARY KEY,

    TransactionId           VARCHAR(200) NULL,
    PostedDate              DATETIMEOFFSET NULL,
    TransactionType         VARCHAR(100) NULL,
    TransactionStatus       VARCHAR(100) NULL,
    AmazonOrderId           VARCHAR(50) NULL,

    SellerSKU               VARCHAR(100) NULL,
    ASIN                    VARCHAR(50) NULL,
    SSKU                    VARCHAR(100) NULL,
    QuantityShipped         INT NULL,

    Principal               DECIMAL(18, 6) NULL,
    ShippingCharges         DECIMAL(18, 6) NULL,
    Promotions              DECIMAL(18, 6) NULL,

    FBAFees                 DECIMAL(18, 6) NULL,
    Commission              DECIMAL(18, 6) NULL,
    FixedClosingFee         DECIMAL(18, 6) NULL,
    VariableClosingFee      DECIMAL(18, 6) NULL,
    ShippingChargeback      DECIMAL(18, 6) NULL,
    RefFee                  DECIMAL(18, 6) NULL,

    Total                   DECIMAL(18, 6) NULL,

    CreatedAt               DATETIMEOFFSET NOT NULL DEFAULT (DATEADD(HOUR,4,SYSDATETIMEOFFSET())),
    UpdatedAt               DATETIMEOFFSET NOT NULL DEFAULT (DATEADD(HOUR,4,SYSDATETIMEOFFSET()))
);
GO
