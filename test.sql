SELECT 
    SUM(Subtotal) AS TotalSales,
    SUM(Profit) AS NetProfit
FROM OrderItems
WHERE OrderDate >= '2026-01-01'
  AND OrderDate <  '2026-02-01';
