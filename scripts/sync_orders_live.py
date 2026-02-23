#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt

from . import config
config.load_env()

from app.orders import get_orders
from app.database import connect_database, replace_order_items_for_order


SYNC_KEY = "ORDERS_LIVE_SYNC"


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


async def fetch_and_upsert():
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    overlap_hours = config.SYNC_OVERLAP_HOURS

    effective_from = (last_sync - dt.timedelta(hours=overlap_hours)).replace(microsecond=0)
    last_updated_after = effective_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    end_dt = dt.datetime.now(dt.timezone.utc)
    last_updated_before = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "LastUpdatedAfter": last_updated_after,
        "LastUpdatedBefore": last_updated_before,
    }

    print("------------------------------------------------------------")
    print("Starting LIVE sync (Orders)")
    print(f"LastSuccessfulSyncUtc: {last_sync.isoformat()}")
    print(f"EffectiveFrom (UTC):   {last_updated_after}")
    print(f"EffectiveTo (UTC):     {last_updated_before}")
    print("------------------------------------------------------------")

    print("Calling get_orders...")
    items = await get_orders(params=params)
    print(f"get_orders returned {len(items) if items else 0} rows")

    if not items:
        update_last_sync_at(end_dt)
        return 0

    grouped = {}
    for row in items:
        oid = row["AmazonOrderId"]
        grouped.setdefault(oid, []).append(row)

    print(f"Upserting {len(grouped)} orders...")

    conn = connect_database()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        for oid, rows in grouped.items():
            replace_order_items_for_order(cur, oid, rows)
        conn.commit()
    except Exception as exc:
        conn.rollback()
        print("ERROR during UPSERT:", exc)
        raise
    finally:
        cur.close()
        conn.close()

    update_last_sync_at(end_dt)
    print("Order sync completed successfully.")
    print("------------------------------------------------------------")

    return len(items)


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()
