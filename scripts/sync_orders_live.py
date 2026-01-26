#!/usr/bin/env python3
r"""
sync_orders_live.py

All-in-one live sync — fetches orders with full enrichment (Brand, Category, Fees).

Place in gcinventory/scripts and run with:
  python gcinventory/scripts/sync_orders_live.py

This file:
 - loads .env from gcinventory/.env
 - formats LastUpdatedAfter using canonical UTC Z format (SP-API expected)
 - persists fetch_end (time when fetch/enrichment completed) into SyncState
 - uses a lockfile and rotating logs so the script can be scheduled directly
"""

import sys
import os
import asyncio
import datetime as dt
import time
import traceback
from dotenv import load_dotenv
import pyodbc
import logging
from logging.handlers import RotatingFileHandler
from typing import List, Dict, Any

# derive repo root dynamically
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# helper to format datetimes as canonical UTC Z strings
def format_dt_z(d: dt.datetime) -> str:
    """Return canonical UTC Z timestamp like 2026-01-26T05:48:16Z."""
    if d is None:
        return None
    if d.tzinfo is None:
        # assume naive datetimes are UTC
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# settings
OVERLAP_HOURS = int(os.getenv("SYNC_OVERLAP_HOURS", "2"))
SQL_CS = os.getenv("SQLSERVER_CONNECTION_STRING")
if not SQL_CS:
    raise RuntimeError("SQLSERVER_CONNECTION_STRING not set")

# lock and logging settings
LOCKFILE = os.path.join(REPO_ROOT, "sync_orders_live.lock")
LOG_DIR = os.path.join(REPO_ROOT, "logs")
LOG_PATH = os.path.join(LOG_DIR, "sync_orders_live.log")
LOCK_TIMEOUT_SECONDS = int(os.getenv("SYNC_LOCK_TIMEOUT_SECONDS", str(6 * 3600)))  # default 6 hours

os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("sync_orders_live")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
console = logging.StreamHandler()
console.setFormatter(formatter)
logger.addHandler(console)

# app imports (project layout: gcinventory/app)
from app.orders import get_orders  # noqa: E402
from app.database import connect_database  # noqa: E402

def utcnow():
    return dt.datetime.now(dt.timezone.utc)

def compute_order_item_key(amazon_order_id, order_item_id, sku, asin):
    order_item_id = (order_item_id or "").strip()
    sku = (sku or "").strip()
    asin = (asin or "").strip()
    if order_item_id and order_item_id != "0":
        return f"{amazon_order_id}:{order_item_id}"
    return f"{amazon_order_id}:0:{sku}:{asin}"

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

def get_db_conn():
    return pyodbc.connect(SQL_CS)

def get_last_sync(cursor):
    cursor.execute("SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE Id = 1")
    row = cursor.fetchone()
    if row and row[0]:
        val = row[0]
        if isinstance(val, dt.datetime):
            if val.tzinfo is None:
                return val.replace(tzinfo=dt.timezone.utc)
            return val.astimezone(dt.timezone.utc)
    return dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)

def update_last_sync_at(ts: dt.datetime):
    if ts is None:
        raise ValueError("ts must be a datetime")
    if ts.tzinfo is None:
        ts_aware = ts.replace(tzinfo=dt.timezone.utc)
    else:
        ts_aware = ts.astimezone(dt.timezone.utc)
    ts_naive_utc = ts_aware.replace(tzinfo=None)
    conn = connect_database()
    cur = conn.cursor()
    try:
        cur.execute("UPDATE spapi_app_user.SyncState SET LastSuccessfulSyncUtc = ? WHERE Id = 1", (ts_naive_utc,))
        if cur.rowcount == 0:
            cur.execute("INSERT INTO spapi_app_user.SyncState (Id, LastSuccessfulSyncUtc) VALUES (1, ?)", (ts_naive_utc,))
        conn.commit()
        logger.info("Persisted LastSuccessfulSyncUtc = %s (fetch_end)", ts_aware.isoformat())
    except Exception:
        conn.rollback()
        logger.exception("ERROR updating SyncState at provided timestamp")
        raise
    finally:
        cur.close()
        conn.close()

# keep original per-row upsert for fallback (omitted here for brevity, same semantics as before)
def upsert_order_item_row(cur, row):
    sql_update = """
    UPDATE spapi_app_user.OrderItems
       SET AmazonOrderId = ?,
           OrderItemId = ?,
           OrderDate = ?,
           SKU = ?,
           ASIN = ?,
           SSKU = ?,
           Brand = ?,
           Category = ?,
           Title = ?,
           Qty = ?,
           UnitPrice = ?,
           Subtotal = ?,
           Currency = ?,
           OrderStatus = ?,
           LastUpdateDate = ?,
           FeeIncl = ?,
           FeePct = ?,
           FBAFeesIncl = ?,
           TotalFee = ?,
           RVAT = ?,
           VAT = ?,
           COG = ?,
           Profit = ?,
           LastSeenAt = SYSUTCDATETIME()
     WHERE OrderItemKey = ?;
    """
    params = (
        row["AmazonOrderId"],
        row["OrderItemId"],
        row["OrderDate"],
        row["SKU"],
        row["ASIN"],
        row["SSKU"],
        row["Brand"],
        row["Category"],
        row["Title"],
        row["Qty"],
        row["UnitPrice"],
        row["Subtotal"],
        row["Currency"],
        row["OrderStatus"],
        row["LastUpdateDate"],
        row.get("FeeIncl"),
        row.get("FeePct"),
        row.get("FBAFeesIncl"),
        row.get("TotalFee"),
        row.get("RVAT"),
        row.get("VAT"),
        row.get("COG"),
        row.get("Profit"),
        row["OrderItemKey"],
    )
    cur.execute(sql_update, params)
    if cur.rowcount == 0:
        sql_insert = """
        INSERT INTO spapi_app_user.OrderItems (
            OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
            SKU, ASIN, SSKU, Brand, Category, Title,
            Qty, UnitPrice, Subtotal, Currency,
            OrderStatus, LastUpdateDate,
            FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
            VAT, COG, Profit,
            FirstSeenAt, LastSeenAt
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME());
        """
        cur.execute(sql_insert, (
            row["OrderItemKey"],
            row["AmazonOrderId"],
            row["OrderItemId"],
            row["OrderDate"],
            row["SKU"],
            row["ASIN"],
            row["SSKU"],
            row["Brand"],
            row["Category"],
            row["Title"],
            row["Qty"],
            row["UnitPrice"],
            row["Subtotal"],
            row["Currency"],
            row["OrderStatus"],
            row["LastUpdateDate"],
            row.get("FeeIncl"),
            row.get("FeePct"),
            row.get("FBAFeesIncl"),
            row.get("TotalFee"),
            row.get("RVAT"),
            row.get("VAT"),
            row.get("COG"),
            row.get("Profit"),
        ))

# bulk upsert uses temp table + MERGE (same as earlier implementation)
def upsert_order_items_bulk(conn, rows: List[Dict[str, Any]]):
    cur = conn.cursor()
    create_temp_sql = """
    CREATE TABLE #OrderItemsStaging (
        OrderItemKey NVARCHAR(300) NOT NULL,
        AmazonOrderId NVARCHAR(100) NULL,
        OrderItemId NVARCHAR(100) NULL,
        OrderDate DATETIME2 NULL,
        SKU NVARCHAR(120) NULL,
        ASIN NVARCHAR(64) NULL,
        SSKU NVARCHAR(120) NULL,
        Brand NVARCHAR(200) NULL,
        Category NVARCHAR(200) NULL,
        Title NVARCHAR(MAX) NULL,
        Qty INT NULL,
        UnitPrice FLOAT NULL,
        Subtotal FLOAT NULL,
        Currency NVARCHAR(10) NULL,
        OrderStatus NVARCHAR(60) NULL,
        LastUpdateDate DATETIME2 NULL,
        FeeIncl FLOAT NULL,
        FeePct FLOAT NULL,
        FBAFeesIncl FLOAT NULL,
        TotalFee FLOAT NULL,
        RVAT FLOAT NULL,
        VAT FLOAT NULL,
        COG FLOAT NULL,
        Profit FLOAT NULL
    );
    """
    cur.execute(create_temp_sql)

    insert_sql = """
    INSERT INTO #OrderItemsStaging (
        OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
        SKU, ASIN, SSKU, Brand, Category, Title,
        Qty, UnitPrice, Subtotal, Currency,
        OrderStatus, LastUpdateDate,
        FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
        VAT, COG, Profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params_list = []
    for r in rows:
        params_list.append((
            r["OrderItemKey"],
            r.get("AmazonOrderId"),
            r.get("OrderItemId"),
            r.get("OrderDate"),
            r.get("SKU"),
            r.get("ASIN"),
            r.get("SSKU"),
            r.get("Brand"),
            r.get("Category"),
            r.get("Title"),
            r.get("Qty"),
            r.get("UnitPrice"),
            r.get("Subtotal"),
            r.get("Currency"),
            r.get("OrderStatus"),
            r.get("LastUpdateDate"),
            r.get("FeeIncl"),
            r.get("FeePct"),
            r.get("FBAFeesIncl"),
            r.get("TotalFee"),
            r.get("RVAT"),
            r.get("VAT"),
            r.get("COG"),
            r.get("Profit"),
        ))
    try:
        cur.fast_executemany = True
    except Exception:
        pass
    cur.executemany(insert_sql, params_list)

    merge_sql = """
    MERGE INTO spapi_app_user.OrderItems AS target
    USING #OrderItemsStaging AS src
      ON target.OrderItemKey = src.OrderItemKey
    WHEN MATCHED THEN
      UPDATE SET
        AmazonOrderId = src.AmazonOrderId,
        OrderItemId = src.OrderItemId,
        OrderDate = src.OrderDate,
        SKU = src.SKU,
        ASIN = src.ASIN,
        SSKU = src.SSKU,
        Brand = src.Brand,
        Category = src.Category,
        Title = src.Title,
        Qty = src.Qty,
        UnitPrice = src.UnitPrice,
        Subtotal = src.Subtotal,
        Currency = src.Currency,
        OrderStatus = src.OrderStatus,
        LastUpdateDate = src.LastUpdateDate,
        FeeIncl = src.FeeIncl,
        FeePct = src.FeePct,
        FBAFeesIncl = src.FBAFeesIncl,
        TotalFee = src.TotalFee,
        RVAT = src.RVAT,
        VAT = src.VAT,
        COG = src.COG,
        Profit = src.Profit,
        LastSeenAt = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
        SKU, ASIN, SSKU, Brand, Category, Title,
        Qty, UnitPrice, Subtotal, Currency,
        OrderStatus, LastUpdateDate,
        FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
        VAT, COG, Profit, FirstSeenAt, LastSeenAt
      )
      VALUES (
        src.OrderItemKey, src.AmazonOrderId, src.OrderItemId, src.OrderDate,
        src.SKU, src.ASIN, src.SSKU, src.Brand, src.Category, src.Title,
        src.Qty, src.UnitPrice, src.Subtotal, src.Currency,
        src.OrderStatus, src.LastUpdateDate,
        src.FeeIncl, src.FeePct, src.FBAFeesIncl, src.TotalFee, src.RVAT,
        src.VAT, src.COG, src.Profit, SYSUTCDATETIME(), SYSUTCDATETIME()
      );
    """
    cur.execute(merge_sql)
    cur.execute("DROP TABLE #OrderItemsStaging")


async def fetch_and_upsert():
    # 1) Read last successful sync from DB
    state_conn = connect_database()
    state_cursor = state_conn.cursor()
    try:
        last_sync = get_last_sync(state_cursor)
    finally:
        state_cursor.close()
        state_conn.close()

    # 2) Compute LastUpdatedAfter = last_sync - overlap
    delta = dt.timedelta(hours=OVERLAP_HOURS)
    effective_from = (last_sync - delta).replace(microsecond=0)
    last_updated_after = format_dt_z(effective_from)
    params = {"LastUpdatedAfter": last_updated_after, "MaxResultsPerPage": 100}

    logger.info("Starting all-in-one sync (with fees). LastSuccessfulSyncUtc=%s OverlapHours=%s Effective LastUpdatedAfter=%s",
                last_sync.isoformat(), OVERLAP_HOURS, last_updated_after)

    # 3) Fetch enriched orders
    fetch_start = time.time()
    conn = connect_database()
    cursor = conn.cursor()
    try:
        items = await get_orders(params=params, db_cursor=cursor)
    finally:
        cursor.close()
        conn.close()
    fetch_elapsed = time.time() - fetch_start
    fetch_end = dt.datetime.now(dt.timezone.utc)
    logger.info("Fetch/enrichment phase completed in %.2f seconds. Items returned: %d", fetch_elapsed, len(items) if items else 0)
    logger.info("Fetch end timestamp (to be persisted): %s", fetch_end.isoformat())

    if not items:
        logger.info("No items returned by get_orders()")
        update_last_sync_at(fetch_end)
        return 0

    # prepare rows for upsert
    rows_to_upsert = []
    for itm in items:
        amazon_order_id = itm.get("AmazonOrderId") or itm.get("oid")
        order_item_id = itm.get("OrderItemId") or itm.get("order_item_id") or ""
        sku = itm.get("SKU") or itm.get("sku")
        asin = itm.get("ASIN") or itm.get("asin")
        ssku = itm.get("SSKU") or itm.get("ssku")
        brand = itm.get("Brand")
        category = itm.get("Category")
        title = itm.get("Title")

        qty = itm.get("Quantity") or itm.get("QuantityOrdered") or itm.get("quantity") or 1
        try:
            qty = int(qty)
        except Exception:
            qty = 1

        unit_price = safe_float(itm.get("SOLD") or itm.get("UnitPrice") or itm.get("unit_price"))
        subtotal = (unit_price * qty) if unit_price else None

        currency = itm.get("Currency") or itm.get("currency")
        order_date = normalize_datetime_for_sql(itm.get("PurchaseDate") or itm.get("OrderDate"))
        last_update = normalize_datetime_for_sql(itm.get("LastUpdateDate") or itm.get("last_update_date"))
        order_status = itm.get("OrderStatus") or itm.get("status")

        fee_incl = safe_float(itm.get("Est Fee"))
        fee_pct = safe_float(itm.get("Est Fee%"))
        fba_fees_incl = safe_float(itm.get("Est FBAFees"))
        total_fee = safe_float(itm.get("Est TotalAmazonFees"))
        r_vat = safe_float(itm.get("Est R. VAT"))
        vat = safe_float(itm.get("VAT"))
        cog = safe_float(itm.get("COG"))
        profit = safe_float(itm.get("Est Net Profit"))

        row = {
            "AmazonOrderId": amazon_order_id,
            "OrderItemId": order_item_id or None,
            "OrderDate": order_date,
            "SKU": sku,
            "ASIN": asin,
            "SSKU": ssku,
            "Brand": brand,
            "Category": category,
            "Title": title,
            "Qty": qty,
            "UnitPrice": unit_price,
            "Subtotal": subtotal,
            "Currency": currency,
            "OrderStatus": order_status,
            "LastUpdateDate": last_update,
            "FeeIncl": fee_incl,
            "FeePct": fee_pct,
            "FBAFeesIncl": fba_fees_incl,
            "TotalFee": total_fee,
            "RVAT": r_vat,
            "VAT": vat,
            "COG": cog,
            "Profit": profit,
        }
        row["OrderItemKey"] = compute_order_item_key(row["AmazonOrderId"] or "", row["OrderItemId"], row["SKU"], row["ASIN"])
        rows_to_upsert.append(row)

    # 4) Upsert into OrderItems (attempt bulk MERGE)
    db_conn = get_db_conn()
    db_conn.autocommit = False
    processed = 0
    try:
        upsert_start = time.time()
        try:
            upsert_order_items_bulk(db_conn, rows_to_upsert)
            db_conn.commit()
            upsert_elapsed = time.time() - upsert_start
            processed = len(rows_to_upsert)
            logger.info("Bulk upsert completed in %.2f seconds for %d rows (avg %.3fs/row).", upsert_elapsed, processed, upsert_elapsed / processed if processed else 0)
        except Exception:
            logger.exception("Bulk upsert failed; falling back to per-row upsert. Rolling back and continuing.")
            try:
                db_conn.rollback()
            except Exception:
                logger.exception("Rollback failed after bulk upsert failure")
            upsert_fallback_start = time.time()
            cur = db_conn.cursor()
            try:
                for row in rows_to_upsert:
                    upsert_order_item_row(cur, row)
                    processed += 1
                db_conn.commit()
            except Exception:
                db_conn.rollback()
                logger.exception("ERROR during fallback per-row upsert")
                raise
            finally:
                cur.close()
            upsert_fallback_elapsed = time.time() - upsert_fallback_start
            logger.info("Fallback per-row upsert completed in %.2f seconds for %d rows (avg %.3fs/row).", upsert_fallback_elapsed, processed, upsert_fallback_elapsed / processed if processed else 0)
    finally:
        db_conn.close()

    # 5) Persist fetch_end as LastSuccessfulSyncUtc
    update_last_sync_at(fetch_end)

    logger.info("All-in-one sync complete. Rows processed: %d", processed)
    return processed


def acquire_lock():
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        fd = os.open(LOCKFILE, flags)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        logger.info("Acquired lock")
        return True
    except FileExistsError:
        try:
            st = os.stat(LOCKFILE)
            age = time.time() - st.st_mtime
            if age > LOCK_TIMEOUT_SECONDS:
                logger.warning("Lockfile is stale (age %.0f s). Removing and acquiring.", age)
                try:
                    os.remove(LOCKFILE)
                except Exception:
                    logger.exception("Failed to remove stale lockfile")
                    return False
                return acquire_lock()
            else:
                logger.info("Lockfile exists and is recent (age %.0f s). Exiting to avoid overlap.", age)
                return False
        except Exception:
            logger.exception("Error inspecting lockfile; refusing to run")
            return False


def release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
            logger.info("Released lock")
    except Exception:
        logger.exception("Failed to remove lockfile on exit")


def main():
    if not acquire_lock():
        logger.info("Another instance is running; exiting.")
        return 0
    try:
        result = asyncio.run(fetch_and_upsert())
        return result
    except Exception:
        logger.exception("Unhandled exception in sync run")
        raise
    finally:
        release_lock()


if __name__ == "__main__":
    logger.info("Starting sync run")
    try:
        exit_code = main() or 0
        logger.info("Sync run finished with code %s", exit_code)
    except SystemExit as e:
        logger.info("Exit: %s", e)
        raise
    except Exception:
        logger.exception("Fatal error in sync runner")
        raise