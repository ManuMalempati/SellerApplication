#!/usr/bin/env python3
"""
sync_orders_live.py

Runs a single live sync:
- Fetch orders via API(app.orders.get_orders)
- Transform rows
- UPSERT each row using robust_upsert_order_items() from database.py
- Maintain LastSuccessfulSyncUtc with overlap to avoid gaps
- Locking to prevent overlapping runs
"""

import sys
import os
import asyncio
import datetime as dt
import time
from dotenv import load_dotenv
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any

# -------------------------------------------------------------------
# Logging (must be first, before ANYTHING else, including print)
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

logger.info("Logger initialized. Writing to %s", LOG_PATH)

def flush_logs():
    for h in logger.handlers:
        try:
            h.flush()
        except Exception:
            pass

# TEMP diagnostic print for Task Scheduler
print("Script startup reached")

# -------------------------------------------------------------------
# Repo / env
# -------------------------------------------------------------------

REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# -------------------------------------------------------------------
# Settings
# -------------------------------------------------------------------

OVERLAP_HOURS = int(os.getenv("SYNC_OVERLAP_HOURS", "2"))
LOCKFILE = os.path.join(REPO_ROOT, "sync_orders_live.lock")
LOCK_TIMEOUT_SECONDS = int(os.getenv("SYNC_LOCK_TIMEOUT_SECONDS", str(6 * 3600)))

# -------------------------------------------------------------------
# App imports
# -------------------------------------------------------------------

from app.orders import get_orders
from app.database import connect_database, robust_upsert_order_items

# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def format_dt_z(d: dt.datetime) -> str:
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def normalize_datetime_for_sql(s):
    if not s:
        return None
    if isinstance(s, dt.datetime):
        return s.replace(tzinfo=None).isoformat(sep=" ")
    if isinstance(s, str):
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1]
        return s.replace("T", " ")
    return None


def safe_float(v):
    if v in (None, "", "Not Available"):
        return None
    try:
        return float(v)
    except Exception:
        return None


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
        logger.info("Persisted LastSuccessfulSyncUtc = %s", ts_aware.isoformat())
    except Exception:
        conn.rollback()
        logger.exception("ERROR updating SyncState")
        raise
    finally:
        cur.close()
        conn.close()

# -------------------------------------------------------------------
# Core sync
# -------------------------------------------------------------------

async def fetch_and_upsert():
    # 1) Read last sync
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    # 2) Compute window
    effective_from = (last_sync - dt.timedelta(hours=OVERLAP_HOURS)).replace(microsecond=0)
    last_updated_after = format_dt_z(effective_from)
    params = {"LastUpdatedAfter": last_updated_after}
    report_end_dt = dt.datetime.now(dt.timezone.utc)  # this will be overwritten

    logger.info(
        "Starting sync\nLastSuccessfulSyncUtc=%s\nOverlapHours=%s\nEffectiveFrom=%s",
        last_sync.isoformat(),
        OVERLAP_HOURS,
        last_updated_after,
    )

    # 3) Fetch report-based items (UPDATED to get items and true report end)
    fetch_start = time.time()
    # Change your get_orders to return: items, end_dt
    result = await get_orders(params=params)
    if isinstance(result, tuple) and len(result) == 2:
        items, report_end_dt = result
    else:
        items = result
        report_end_dt = dt.datetime.now(dt.timezone.utc)  # fallback

    logger.info(
        "Fetch completed in %.2fs. Items returned: %d",
        time.time() - fetch_start,
        len(items) if items else 0,
    )

    if not items:
        update_last_sync_at(report_end_dt)  # <-- Use report's window end
        flush_logs()
        return 0

    # 4) UPSERT each row
    conn = connect_database()
    conn.autocommit = False
    cur = conn.cursor()
    processed = 0

    try:
        for itm in items:
            # Use OrderItemKey from orders.py (row-index based)
            key = itm.get("OrderItemKey")
            if not key:
                logger.error("Missing OrderItemKey in item: %s", itm)
                continue

            row = {
                "OrderItemKey": key,
                "AmazonOrderId": itm.get("AmazonOrderId"),
                "OrderDate": normalize_datetime_for_sql(itm.get("OrderDate")),
                "SKU": itm.get("SKU"),
                "ASIN": itm.get("ASIN"),
                "SSKU": itm.get("SSKU"),
                "Brand": itm.get("Brand"),
                "Category": itm.get("Category"),
                "Title": itm.get("Title"),
                "Qty": itm.get("Qty"),
                "UnitPrice": itm.get("UnitPrice"),
                "Subtotal": itm.get("Subtotal"),
                "Currency": itm.get("Currency"),
                "OrderStatus": itm.get("OrderStatus"),
                "LastUpdateDate": normalize_datetime_for_sql(itm.get("LastUpdateDate")),
                "FeeIncl": itm.get("FeeIncl"),
                "FeePct": itm.get("FeePct"),
                "FBAFeesIncl": itm.get("FBAFeesIncl"),
                "TotalFee": itm.get("TotalFee"),
                "RVAT": itm.get("RVAT"),
                "VAT": itm.get("VAT"),
                "COG": itm.get("COG"),
                "Profit": itm.get("Profit"),
            }

            ok = robust_upsert_order_items(cur, row)
            if ok:
                processed += 1

        conn.commit()
    except Exception:
        conn.rollback()
        logger.exception("ERROR during UPSERT")
        raise
    finally:
        cur.close()
        conn.close()

    # 5) Save the TRUE report window end
    update_last_sync_at(report_end_dt)
    logger.info("Sync complete. Rows processed: %d", processed)
    flush_logs()
    return processed

# -------------------------------------------------------------------
# Locking
# -------------------------------------------------------------------

def acquire_lock():
    try:
        fd = os.open(LOCKFILE, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        logger.info("Acquired lock")
        flush_logs()
        return True
    except FileExistsError:
        try:
            age = time.time() - os.stat(LOCKFILE).st_mtime
            if age > LOCK_TIMEOUT_SECONDS:
                logger.warning("Stale lockfile (%.0fs). Removing.", age)
                os.remove(LOCKFILE)
                return acquire_lock()
            logger.info("Lockfile exists (%.0fs). Exiting.", age)
            flush_logs()
            return False
        except Exception:
            logger.exception("Error inspecting lockfile")
            flush_logs()
            return False


def release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
            logger.info("Released lock")
            flush_logs()
    except Exception:
        logger.exception("Failed to remove lockfile")
        flush_logs()

# -------------------------------------------------------------------
# Entry
# -------------------------------------------------------------------

def main():
    logger.info("Starting sync run")
    flush_logs()
    if not acquire_lock():
        return 0
    try:
        result = asyncio.run(fetch_and_upsert())
        return result
    finally:
        release_lock()
        logger.info("Sync finished")
        flush_logs()

if __name__ == "__main__":
    try:
        rc = main()
        flush_logs()
        sys.exit(0 if rc else 1)   # 0 for success, 1 for failure
    except Exception:
        logger.exception("Fatal error in sync_orders_live")
        flush_logs()
        sys.exit(1)