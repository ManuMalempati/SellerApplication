UPDATE spapi_app_user.SyncState
SET LastSuccessfulSyncUtc = DATEADD(day, -3, SYSUTCDATETIME())
WHERE Id = 1;
