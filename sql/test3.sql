SELECT T.*
FROM spapi_app_user.FinancialTransactions T
JOIN (
    SELECT 
        AmazonOrderId,
        TransactionType,
        SellerSKU,
        SSKU,
        ASIN
    FROM 
        spapi_app_user.FinancialTransactions
    GROUP BY 
        AmazonOrderId,
        TransactionType,
        SellerSKU,
        SSKU,
        ASIN
    HAVING 
        SUM(CASE WHEN TransactionStatus = 'DEFERRED_RELEASED' THEN 1 ELSE 0 END) > 0
        AND
        SUM(CASE WHEN TransactionStatus = 'DEFERRED' THEN 1 ELSE 0 END) > 0
) X
  ON T.AmazonOrderId   = X.AmazonOrderId
 AND T.TransactionType = X.TransactionType
 AND T.SellerSKU       = X.SellerSKU
 AND T.SSKU            = X.SSKU
 AND T.ASIN            = X.ASIN
ORDER BY 
    T.AmazonOrderId,
    T.TransactionType,
    T.SellerSKU,
    T.TransactionStatus;
