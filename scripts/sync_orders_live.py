#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt
import time

from dotenv import load_dotenv
from app.orders import get_orders
from app.database import connect_database, replace_order_items_for_order

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def get_last_sync(cursor) -> dt.datetime:
    cursor.execute("SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE Id = 1")
    row = cursor.fetchone()
    default = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)
    if not row or not row[0]:
        return default
    val = row[0]
    # If DB returned a string
    if isinstance(val, str):
        cleaned = val.strip().replace("\u200b", "").replace("\ufeff", "")
        try:
            parsed = dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except:
            return default
    # If DB returned a datetime
    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)
    return default

def update_last_sync_at(ts: dt.datetime):
    if ts.tzinfo is None:
        ts_aware = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts_aware = ts.astimezone(dt.timezone.utc)
    ts_naive = ts_aware.replace(tzinfo=None)
    conn = connect_database()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE spapi_app_user.SyncState SET LastSuccessfulSyncUtc = ? WHERE Id = 1",
            (ts_naive,),
        )
        if cur.rowcount == 0:
            cur.execute(
                "INSERT INTO spapi_app_user.SyncState (Id, LastSuccessfulSyncUtc) VALUES (1, ?)",
                (ts_naive,),
            )
        conn.commit()
    except Exception as exc:
        print("ERROR updating SyncState:", exc)
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# -------------------------------------------------------------------
# Main Sync Logic
# -------------------------------------------------------------------

async def fetch_and_upsert():
    # Load last sync
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    # Overlap hours from env
    overlap_hours = int(os.getenv("SYNC_OVERLAP_HOURS", "2"))

    effective_from = (last_sync - dt.timedelta(hours=overlap_hours)).replace(microsecond=0)
    last_updated_after = effective_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Use current time for the sync end
    end_dt = dt.datetime.now(dt.timezone.utc)
    last_updated_before = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "LastUpdatedAfter": last_updated_after,
        "LastUpdatedBefore": last_updated_before,
    }

    print("------------------------------------------------------------")
    print("Starting LIVE sync")
    print(f"LastSuccessfulSyncUtc: {last_sync.isoformat()}")
    print(f"EffectiveFrom (UTC):   {last_updated_after}")
    print(f"EffectiveTo (UTC):     {last_updated_before}")
    print("------------------------------------------------------------")

    # Fetch orders
    print("Calling get_orders...")
    items = await get_orders(params=params)
    print(f"get_orders returned {len(items) if items else 0} rows")

    if not items:
        print("No items returned. Updating sync state and exiting.")
        update_last_sync_at(end_dt)  # <-- use actual now (upper window end)
        return 0

    # Group rows by order
    grouped = {}
    for row in items:
        oid = row["AmazonOrderId"]
        grouped.setdefault(oid, []).append(row)

    print(f"Upserting {len(grouped)} orders...")

    # DB insert/replace
    conn = connect_database()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        for oid, rows in grouped.items():
            replace_order_items_for_order(cur, oid, rows)
        conn.commit()
        print(f"Finished upserting {len(grouped)} orders.")
    except Exception as exc:
        conn.rollback()
        print("ERROR during UPSERT:", exc)
        raise
    finally:
        cur.close()
        conn.close()

    # Update sync state
    update_last_sync_at(end_dt)  # <-- use actual now (upper window end)
    print("Sync completed successfully.")
    print("------------------------------------------------------------")

    return len(items)

def main():
    asyncio.run(fetch_and_upsert())

if __name__ == "__main__":
    main()