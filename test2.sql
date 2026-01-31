SELECT *
FROM spapi_app_user.OrderItems
WHERE CAST(OrderDate AS DATE) = CAST(GETUTCDATE() AS DATE)
ORDER BY OrderDate DESC;
