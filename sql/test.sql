SELECT TOP 5 SKU, Title, SUM(Subtotal) AS Total FROM OrderItems
GROUP BY SKU, Title
ORDER BY Total DESC
