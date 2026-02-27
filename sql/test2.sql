UPDATE spapi_app_user.SyncState
SET LastSuccessfulSyncUtc = DATEADD(DAY, -1, LastSuccessfulSyncUtc)
WHERE SyncKey = 'ORDERS_LIVE_SYNC';
