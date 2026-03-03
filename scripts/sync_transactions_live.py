#!/usr/bin/env python3
import asyncio
import datetime as dt
import config
from app.transactions.transactions import get_transactions
from app.database import connect_database
from app.transactions.database_transactions import upsert_financial_transactions

# Standardized helpers from utils
from app.utilities.utils import (
    convert_utc_to_utcz_string, 
    get_now_iso_string_with_custom_utc_offset
)

SYNC_KEY = "TRANSACTIONS_LIVE_SYNC"

# ---------------------------------------------------------
# SyncState Helpers
# ---------------------------------------------------------

def get_last_sync(cursor) -> dt.datetime:
    """
    Read LastSuccessfulSyncUtc from DB.
    Stored as naive UTC datetime. Returned as aware UTC datetime.
    """
    cursor.execute(
        "SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE SyncKey = ?",
        (SYNC_KEY,)
    )
    row = cursor.fetchone()
    default = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)

    if not row or not row[0]:
        return default

    val = row[0]
    if isinstance(val, dt.datetime):
        return val.replace(tzinfo=dt.timezone.utc)

    try:
        parsed = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
        return parsed.astimezone(dt.timezone.utc)
    except:
        return default


def update_last_sync_at(ts: dt.datetime):
    """Store sync cursor as naive UTC datetime."""
    ts_utc = ts.astimezone(dt.timezone.utc)
    ts_naive = ts_utc.replace(tzinfo=None)

    conn = connect_database()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE spapi_app_user.SyncState SET LastSuccessfulSyncUtc = ? WHERE SyncKey = ?",
            (ts_naive, SYNC_KEY)
        )
        if cur.rowcount == 0:
            cur.execute(
                "INSERT INTO spapi_app_user.SyncState (SyncKey, LastSuccessfulSyncUtc) VALUES (?, ?)",
                (SYNC_KEY, ts_naive)
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


# ---------------------------------------------------------
# LIVE SYNC
# ---------------------------------------------------------

async def fetch_and_upsert():
    # 1. Load sync state
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    # 2. Determine time range
    overlap_hours = config.SYNC_OVERLAP_HOURS
    effective_from = last_sync - dt.timedelta(hours=overlap_hours)
    
    now_utc = dt.datetime.now(dt.timezone.utc)
    # 2-minute buffer for Amazon data propagation
    safe_end = now_utc - dt.timedelta(minutes=2) 

    # Using standardized Zulu converter
    posted_after = convert_utc_to_utcz_string(effective_from)
    posted_before = convert_utc_to_utcz_string(safe_end)

    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
    }

    print("------------------------------------------------------------")
    # Using standardized local logging string
    log_ts = get_now_iso_string_with_custom_utc_offset()
    print(f"[{log_ts}] Starting LIVE transaction sync")
    print(f"Last Sync (UTC):       {last_sync.isoformat()}")
    print(f"Range Start (UTC Z):   {posted_after}")
    print(f"Range End (UTC Z):     {posted_before}")
    print("------------------------------------------------------------")

    # 3. Fetch Transactions
    conn = connect_database()
    cur = conn.cursor()
    try:
        rows = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    print(f"get_transactions returned {len(rows) if rows else 0} rows")

    # 4. Handle Empty State
    if not rows:
        update_last_sync_at(safe_end)
        log_ts = get_now_iso_string_with_custom_utc_offset()
        print(f"[{log_ts}] No new data. Sync cursor moved forward.")
        return 0

    # 5. Fast Upsert via shared optimized logic
    print(f"Upserting {len(rows)} transactions...")
    upserted = upsert_financial_transactions(rows)

    # 6. Success: Move sync cursor forward
    update_last_sync_at(safe_end)

    log_ts = get_now_iso_string_with_custom_utc_offset()
    print(f"[{log_ts}] Sync successful. Upserted {upserted} rows.")
    print("------------------------------------------------------------")

    return upserted


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()