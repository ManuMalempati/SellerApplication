-- SQL Server table aligned to your report columns (normalized names)
-- Schema: spapi_app_user
-- All date/time fields stored as exact timestamps using DATETIME2.

CREATE TABLE spapi_app_user.OrderItems (
    id BIGINT IDENTITY(1,1) NOT NULL CONSTRAINT PK_OrderItems PRIMARY KEY CLUSTERED,

    -- Computed upsert key (required for safe upserts)
    OrderItemKey NVARCHAR(300) NOT NULL,

    -- Core identifiers
    AmazonOrderId NVARCHAR(30) NOT NULL,
    OrderItemId NVARCHAR(30) NULL,               -- from Orders API; may be absent in some reports

    -- Order timestamp (exact)
    OrderDate DATETIME2(0) NULL,                 -- PurchaseDate -> OrderDate

    SKU NVARCHAR(80) NULL,
    ASIN NVARCHAR(20) NULL,
    SSKU NVARCHAR(80) NULL,

    Brand NVARCHAR(120) NULL,
    Category NVARCHAR(200) NULL,
    Title NVARCHAR(500) NULL,

    Qty INT NULL,

    UnitPrice DECIMAL(18, 4) NULL,
    Subtotal DECIMAL(18, 4) NULL,
    Currency NVARCHAR(10) NULL,

    FeeIncl DECIMAL(18, 6) NULL,                 -- "Fee incl"
    FeePct DECIMAL(18, 6) NULL,                  -- "Fee %"
    FBAFeesIncl DECIMAL(18, 6) NULL,             -- "FBAFees Incl"
    TotalFee DECIMAL(18, 6) NULL,                -- "Total Fee"
    RVAT DECIMAL(18, 6) NULL,                    -- "R. VAT"
    VAT DECIMAL(18, 6) NULL,
    COG DECIMAL(18, 6) NULL,
    Profit DECIMAL(18, 6) NULL,

    -- Refund / return (exact timestamps)
    Refund NVARCHAR(80) NULL,                    -- "Refund" (id/reference)
    RefundDate DATETIME2(0) NULL,                -- "Ref-Date"
    ReturnDate DATETIME2(0) NULL,                -- "Ret-Date"
    ReturnDisposition NVARCHAR(120) NULL,         -- "Ret-Disposition"
    ReturnReason NVARCHAR(300) NULL,              -- "Ret-reason"

    -- FBA inventory / reimbursement / removal
    LicensePlateNumber NVARCHAR(80) NULL,         -- "license-plate-number"
    Reimbursed BIT NULL,
    ReimbDate DATETIME2(0) NULL,                 -- "Reimb-Date"

    RemovalDate DATETIME2(0) NULL,
    RemovalId NVARCHAR(80) NULL,                 -- "Rem-ID"
    RemovalTracking NVARCHAR(120) NULL,           -- "Rem-Tracking"
    RemovalDelivery NVARCHAR(120) NULL,           -- "Rem-Delivery"

    -- Orders API operational fields (exact timestamps)
    OrderStatus NVARCHAR(30) NULL,
    LastUpdateDate DATETIME2(0) NULL,

    -- Ingestion timestamps (exact)
    FirstSeenAt DATETIME2(0) NOT NULL CONSTRAINT DF_OrderItems_FirstSeenAt DEFAULT (SYSUTCDATETIME()),
    LastSeenAt  DATETIME2(0) NOT NULL CONSTRAINT DF_OrderItems_LastSeenAt  DEFAULT (SYSUTCDATETIME())
);

CREATE UNIQUE INDEX UX_OrderItems_OrderItemKey
ON spapi_app_user.OrderItems (OrderItemKey);

CREATE INDEX IX_OrderItems_OrderDate
ON spapi_app_user.OrderItems (OrderDate DESC)
INCLUDE (AmazonOrderId, SKU, ASIN, OrderStatus, Qty, UnitPrice, Currency);

CREATE INDEX IX_OrderItems_LastUpdateDate
ON spapi_app_user.OrderItems (LastUpdateDate DESC)
INCLUDE (AmazonOrderId, SKU, ASIN, OrderStatus);