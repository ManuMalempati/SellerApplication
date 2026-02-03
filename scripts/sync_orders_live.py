#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt
import time
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler

from app.orders import get_orders
from app.database import connect_database, replace_order_items_for_order  # <-- now use replace fn!

# -------------------------------------------------------------------
# Logging setup (unchanged)
# -------------------------------------------------------------------

LOG_DIR = os.path.join(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "sync_orders_live.log")

logger = logging.getLogger("sync_orders_live")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(
    LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8"
)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

def flush_logs():
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass


def format_dt_z(d: dt.datetime) -> str:
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def get_last_sync(cursor) -> dt.datetime:
    cursor.execute("SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE Id = 1")
    row = cursor.fetchone()
    if row and row[0]:
        val = row[0]
        if val.tzinfo is None:
            return val.replace(tzinfo=dt.timezone.utc)
        return val.astimezone(dt.timezone.utc)
    return dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)


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
    except Exception:
        conn.rollback()
        logger.exception("ERROR updating SyncState")
        raise
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

    effective_from = (last_sync - dt.timedelta(hours=2)).replace(microsecond=0)
    last_updated_after = format_dt_z(effective_from)
    params = {"LastUpdatedAfter": last_updated_after}
    report_end_dt = dt.datetime.now(dt.timezone.utc)  # fallback

    logger.info("Starting sync. LastSuccessfulSyncUtc=%s EffectiveFrom=%s", last_sync.isoformat(), last_updated_after)
    flush_logs()

    items = await get_orders(params=params)
    if not items:
        update_last_sync_at(report_end_dt)
        flush_logs()
        return 0

    # === GROUP-BY-ORDER DELETE-AND-REPLACE LOGIC STARTS HERE ===
    grouped = {}
    for row in items:
        oid = row["AmazonOrderId"]
        grouped.setdefault(oid, []).append(row)

    conn = connect_database()
    conn.autocommit = False
    cur = conn.cursor()
    try:
        for oid, rows in grouped.items():
            replace_order_items_for_order(cur, oid, rows)
        conn.commit()
        logger.info("Finished upserting %d orders (atomic replace by order)", len(grouped))
        flush_logs()
    except Exception:
        conn.rollback()
        logger.exception("ERROR during UPSERT")
        flush_logs()
        raise
    finally:
        cur.close()
        conn.close()

    # Update sync time at end
    update_last_sync_at(report_end_dt)
    logger.info("Sync completed")
    flush_logs()
    return len(items)

def main():
    asyncio.run(fetch_and_upsert())

if __name__ == "__main__":
    main()