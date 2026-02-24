------------------------------------------------------------
-- DROP TABLE (safe)
------------------------------------------------------------
IF OBJECT_ID('spapi_app_user.FBACustomerReturns', 'U') IS NOT NULL
    DROP TABLE spapi_app_user.FBACustomerReturns;


------------------------------------------------------------
-- RECREATE TABLE (DATETIME ONLY — NO OFFSET)
------------------------------------------------------------
CREATE TABLE spapi_app_user.FBACustomerReturns (
    id INT IDENTITY(1,1) PRIMARY KEY,

    return_date DATETIME,                 -- UTC+4 naive
    order_id NVARCHAR(50),
    sku NVARCHAR(200),
    asin NVARCHAR(20),
    fnsku NVARCHAR(50),
    license_plate_number NVARCHAR(200),

    product_name NVARCHAR(1000),
    quantity INT,
    fulfillment_center_id NVARCHAR(50),
    detailed_disposition NVARCHAR(200),
    reason NVARCHAR(500),
    customer_comments NVARCHAR(MAX),

    created_at DATETIME,                  -- UTC+4 naive
    updated_at DATETIME                   -- UTC+4 naive
);


------------------------------------------------------------
-- RECOMMENDED INDEXES
------------------------------------------------------------
CREATE INDEX IX_FBACustomerReturns_OrderSku
ON spapi_app_user.FBACustomerReturns (order_id, sku);
