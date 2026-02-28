#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt

from . import config
config.load_env()

from app.orders import get_orders
from app.database import connect_database, replace_order_items_for_order

# Standardized helpers from your config
from config import (
    convert_utc_to_utcz_string, 
    get_now_iso_string_with_custom_utc_offset
)

SYNC_KEY = "ORDERS_LIVE_SYNC"

# -------------------------------------------------------------------
# Sync State Helpers
# -------------------------------------------------------------------

def get_last_sync(cursor) -> dt.datetime:
    cursor.execute(
        "SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE SyncKey = ?",
        (SYNC_KEY,)
    )
    row = cursor.fetchone()
    default = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)

    if not row or not row[0]:
        return default

    val = row[0]
    if isinstance(val, str):
        cleaned = val.strip().replace("\u200b", "").replace("\ufeff", "")
        try:
            parsed = dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except:
            return default

    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)

    return default

def update_last_sync_at(ts: dt.datetime):
    """Saves the sync timestamp back to DB as naive UTC."""
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

# -------------------------------------------------------------------
# Main Logic
# -------------------------------------------------------------------

async def fetch_and_upsert():
    # 1. Fetch current sync state
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
    end_dt = dt.datetime.now(dt.timezone.utc)

    # Using standardized Zulu converter
    last_updated_after = convert_utc_to_utcz_string(effective_from)
    last_updated_before = convert_utc_to_utcz_string(end_dt)

    params = {
        "LastUpdatedAfter": last_updated_after,
        "LastUpdatedBefore": last_updated_before,
    }

    print("------------------------------------------------------------")
    # Using standardized local logging string
    log_ts = get_now_iso_string_with_custom_utc_offset()
    print(f"[{log_ts}] Starting LIVE sync (Orders)")
    print(f"Last Sync (UTC):       {last_sync.isoformat()}")
    print(f"Range Start (UTC Z):   {last_updated_after}")
    print(f"Range End (UTC Z):     {last_updated_before}")
    print("------------------------------------------------------------")

    # 3. API Fetch
    print("Calling get_orders...")
    items = await get_orders(params=params)
    print(f"get_orders returned {len(items) if items else 0} rows")

    # 4. Handle results
    if not items:
        update_last_sync_at(end_dt)
        log_ts = get_now_iso_string_with_custom_utc_offset()
        print(f"[{log_ts}] No new items. Updated sync state.")
        return 0

    # Group by Order ID for the replace-and-insert logic
    grouped = {}
    for row in items:
        oid = row["AmazonOrderId"]
        grouped.setdefault(oid, []).append(row)

    print(f"Upserting {len(grouped)} unique orders...")

    # 5. DB Write
    conn = connect_database()
    conn.autocommit = False # Using transaction for safety
    cur = conn.cursor()

    try:
        for oid, rows in grouped.items():
            replace_order_items_for_order(cur, oid, rows)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        log_ts = get_now_iso_string_with_custom_utc_offset()
        print(f"[{log_ts}] FATAL: Database error during upsert.")
        raise exc # Crash the script as requested
    finally:
        cur.close()
        conn.close()

    # 6. Finalize
    update_last_sync_at(end_dt)
    log_ts = get_now_iso_string_with_custom_utc_offset()
    print(f"[{log_ts}] Order sync completed successfully.")
    print("------------------------------------------------------------")

    return len(items)

def main():
    asyncio.run(fetch_and_upsert())

if __name__ == "__main__":
    main()