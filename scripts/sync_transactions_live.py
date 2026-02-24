#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database
from app.database import upsert_financial_transactions   # shared optimized upsert


SYNC_KEY = "TRANSACTIONS_LIVE_SYNC"


# ---------------------------------------------------------
# SyncState Helpers
# ---------------------------------------------------------

def get_last_sync(cursor) -> dt.datetime:
    """
    Read LastSuccessfulSyncUtc from DB.
    Stored as naive UTC datetime.
    Returned as aware UTC datetime.
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

    # val is naive UTC datetime → attach UTC tzinfo
    if isinstance(val, dt.datetime):
        return val.replace(tzinfo=dt.timezone.utc)

    # val is string (rare)
    try:
        parsed = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
        return parsed.astimezone(dt.timezone.utc)
    except:
        return default


def update_last_sync_at(ts: dt.datetime):
    """
    Store sync cursor as naive UTC datetime.
    """
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
    # Load last sync cursor
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    overlap_hours = config.SYNC_OVERLAP_HOURS
    effective_from = last_sync - dt.timedelta(hours=overlap_hours)

    posted_after = effective_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    now_utc = dt.datetime.now(dt.timezone.utc)
    safe_end = now_utc - dt.timedelta(minutes=2)
    posted_before = safe_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
    }

    print("------------------------------------------------------------")
    print("Starting LIVE transaction sync")
    print(f"LastSuccessfulSyncUtc: {last_sync.isoformat()}")
    print(f"EffectiveFrom (UTC):   {posted_after}")
    print(f"EffectiveTo (UTC):     {posted_before}")
    print("------------------------------------------------------------")

    # Fetch transactions
    conn = connect_database()
    cur = conn.cursor()
    try:
        rows = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    print(f"get_transactions returned {len(rows) if rows else 0} rows")

    if not rows:
        update_last_sync_at(safe_end)
        return 0

    print("Fast upserting transactions via shared upsert...")

    # ⭐ Use shared optimized lifecycle-aware upsert
    upserted = upsert_financial_transactions(rows)

    # Move sync cursor forward
    update_last_sync_at(safe_end)

    print(f"Transaction sync completed successfully. Upserted {upserted} rows.")
    print("------------------------------------------------------------")

    return upserted


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()