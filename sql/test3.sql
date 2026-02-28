DROP TABLE IF EXISTS spapi_app_user.FBARemovalOrders;

CREATE TABLE spapi_app_user.FBARemovalOrders (
    id INT IDENTITY(1,1) PRIMARY KEY,

    order_id               NVARCHAR(200)   NOT NULL,
    sku                    NVARCHAR(500)   NULL,
    disposition            NVARCHAR(500)   NULL,
    request_date           DATETIME        NULL,
    order_type             NVARCHAR(200)   NULL,
    service_speed          NVARCHAR(200)   NULL,
    order_status           NVARCHAR(200)   NULL,
    last_updated_date      DATETIME        NULL,
    fnsku                  NVARCHAR(200)   NULL,

    requested_quantity     INT             NULL,
    cancelled_quantity     INT             NULL,
    disposed_quantity      INT             NULL,
    shipped_quantity       INT             NULL,
    in_process_quantity    INT             NULL,

    removal_fee            FLOAT           NULL,
    currency               NVARCHAR(50)    NULL,

    created_at             DATETIME        NULL,
    updated_at             DATETIME        NULL
);

-- Optional but recommended indexes
CREATE INDEX IX_FBARemovalOrders_OrderId ON spapi_app_user.FBARemovalOrders(order_id);
CREATE INDEX IX_FBARemovalOrders_SKU ON spapi_app_user.FBARemovalOrders(sku);