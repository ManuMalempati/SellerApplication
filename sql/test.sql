SELECT AmazonOrderId FROM OrderItems
WHERE Qty > 1
GROUP BY AmazonOrderId
HAVING COUNT(SKU) > 1
