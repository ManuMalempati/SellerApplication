SELECT 
    SKU,
    COUNT(*) AS TotalOrderItems_L30,
    SUM(Subtotal) AS OrderedProductSales_L30
    --SUM(CASE WHEN Refund = 'Yes' THEN 1 ELSE 0 END) AS UnitsRefunded_L30
FROM OrderItems
WHERE OrderDate >= DATEADD(DAY, -30, GETDATE())
  AND OrderStatus != 'Cancelled'
GROUP BY SKU
ORDER BY OrderedProductSales_L30 DESC;