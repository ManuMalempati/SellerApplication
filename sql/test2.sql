UPDATE spapi_app_user.SyncState
SET LastSuccessfulSyncUtc = DATEADD(HOUR, -10, LastSuccessfulSyncUtc)
WHERE SyncKey = 'ORDERS_LIVE_SYNC';
