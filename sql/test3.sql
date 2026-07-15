SELECT AmazonOrderId FROM FinancialTransactions
WHERE TransactionType != 'Refund'
GROUP BY AmazonOrderId
HAVING COUNT(*) > 1