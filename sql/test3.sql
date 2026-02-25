UPDATE spapi_app_user.SyncState
SET LastSuccessfulSyncUtc = DATEADD(DAY, -3, LastSuccessfulSyncUtc)
WHERE SyncKey = 'TRANSACTIONS_LIVE_SYNC';