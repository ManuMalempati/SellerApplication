SELECT AmazonOrderId
FROM OrderItems
WHERE OrderStatus IN ('Pending', 'Shipped')
GROUP BY AmazonOrderId
HAVING COUNT(DISTINCT OrderStatus) = 2;
