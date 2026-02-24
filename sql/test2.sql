SELECT 
    AmazonOrderId,
    TransactionType,
    SellerSKU,
    ASIN,
    SSKU,
    COUNT(*) AS Cnt,
    SUM(CASE WHEN TransactionStatus = 'DEFERRED' THEN 1 ELSE 0 END) AS DeferredCount,
    SUM(CASE WHEN TransactionStatus = 'RELEASED' THEN 1 ELSE 0 END) AS ReleasedCount
FROM spapi_app_user.FinancialTransactions
GROUP BY 
    AmazonOrderId,
    TransactionType,
    SellerSKU,
    ASIN,
    SSKU
HAVING 
    SUM(CASE WHEN TransactionStatus = 'DEFERRED' THEN 1 ELSE 0 END) > 0
    AND
    SUM(CASE WHEN TransactionStatus = 'RELEASED' THEN 1 ELSE 0 END) > 0;
