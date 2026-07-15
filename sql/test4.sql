
SELECT * FROM spapi_app_user.InventoryLedger
WHERE EventType = 'Adjustments'
AND Reason IN ('M','5','E','6')