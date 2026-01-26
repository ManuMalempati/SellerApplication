#!/usr/bin/env python3
"""
backfill_orders.py

One-off backfill for Orders over a historical period using SP-API Reports.

- Designed to be launched once from Task Scheduler and run until completion (can continue if you log off).
- Acquires a global backfill lock so inventorysync knows to pause/skips runs while backfill is active.
- If inventorysync is running when backfill starts, backfill will wait for it to finish (up to a timeout) to avoid table contention.
- Respects SP-API report/document rate limits via retry/backoff wrappers.
"""
from __future__ import annotations
import argparse
import sys
import os
import time
import datetime as dt
import csv
import gzip
import logging
import requests
import re
from typing import List, Dict, Tuple, Any, Optional, Callable
from dotenv import load_dotenv

# repo root
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# Required envs
REQUIRED_ENVS = ["LWA_TOKEN_URL", "LWA_CLIENT_ID", "LWA_CLIENT_SECRET", "LWA_REFRESH_TOKEN", "SPAPI_ENDPOINT", "MARKETPLACE_ID", "SQLSERVER_CONNECTION_STRING"]
missing_envs = [k for k in REQUIRED_ENVS if not os.getenv(k)]
if missing_envs:
    raise RuntimeError("Missing required env vars: " + ", ".join(missing_envs))

# imports from app
from app.auth import spapi_request
from app.database import (
    connect_database,
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    upsert_fee_estimate_to_product_mapping,
    get_fee_estimate_from_product_mapping,
)
from app.estimates import get_fees_estimate
from app.orders import get_listing_prices_batch
import json
import pyodbc

# Logging
LOG_DIR = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
logger = logging.getLogger("backfill_orders")
logger.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(ch)
fh = logging.FileHandler(os.path.join(LOG_DIR, "backfill_orders.log"))
fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(fh)

# Config
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"
REPORT_API_PREFIX = "/reports/2021-06-30"
DEFAULT_CHUNK_DAYS = int(os.getenv("BACKFILL_CHUNK_DAYS", "7"))
FEE_CACHE_TTL_DAYS = int(os.getenv("FEE_CACHE_TTL_DAYS", "7"))
MAX_REPORT_POLL_SECONDS = int(os.getenv("REPORT_POLL_MAX_SECONDS", "600"))
REPORT_POLL_INTERVAL = float(os.getenv("REPORT_POLL_INTERVAL", "5"))
BATCH_INSERT_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "1000"))

MAX_RETRIES = int(os.getenv("BACKFILL_MAX_RETRIES", "5"))
INITIAL_RETRY_DELAY = float(os.getenv("BACKFILL_INITIAL_RETRY_DELAY", "5.0"))

# Lock coordination
BACKFILL_LOCKFILE = os.path.join(REPO_ROOT, "backfill.lock")
INVENTORY_LOCKFILE = os.path.join(REPO_ROOT, "inventorysync.lock")
# When backfill starts and finds inventorysync.lock present, wait up to this many seconds for inventory to finish
WAIT_FOR_INVENTORY_SECONDS = int(os.getenv("BACKFILL_WAIT_FOR_INVENTORY_SECONDS", str(10 * 60)))  # default 10 minutes
LOCK_TIMEOUT_SECONDS = int(os.getenv("BACKFILL_LOCK_TIMEOUT_SECONDS", str(24 * 3600)))  # backfill lock stale threshold

# Helpers
_norm_re = re.compile(r"[^a-z0-9]")
def normalize_col_name(s: str) -> str:
    if s is None:
        return ""
    return _norm_re.sub("", s.strip().lower())

def format_dt_z(d: dt.datetime) -> str:
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# Retry wrappers
def retry_spapi_call(fn: Callable[..., Dict[str, Any]], *args, max_retries: int = MAX_RETRIES, initial_delay: float = INITIAL_RETRY_DELAY, **kwargs) -> Dict[str, Any]:
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        resp = fn(*args, **kwargs)
        if not isinstance(resp, dict) or "errors" not in resp:
            return resp
        err_codes = [e.get("code") for e in resp.get("errors", []) if isinstance(e, dict)]
        if "QuotaExceeded" in err_codes or "RequestThrottled" in err_codes or "TooManyRequests" in err_codes:
            if attempt < max_retries:
                logger.warning("SP-API rate limit '%s' hit (attempt %d/%d). Backing off %.1fs and retrying.", err_codes, attempt, max_retries, delay)
                time.sleep(delay)
                delay *= 2
                continue
            else:
                logger.error("SP-API rate limit persisted after %d attempts: %s", max_retries, err_codes)
                return resp
        return resp
    return fn(*args, **kwargs)

def retry_requests_get(url: str, timeout: int = 60, max_retries: int = MAX_RETRIES, initial_delay: float = INITIAL_RETRY_DELAY, **kwargs) -> requests.Response:
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(url, timeout=timeout, **kwargs)
            if r.status_code in (429, 503, 500):
                if attempt < max_retries:
                    logger.warning("Download returned %d. Backoff %.1fs (attempt %d/%d) and retrying.", r.status_code, delay, attempt, max_retries)
                    time.sleep(delay)
                    delay *= 2
                    continue
                else:
                    r.raise_for_status()
            r.raise_for_status()
            return r
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                logger.warning("Download error '%s' (attempt %d/%d). Backoff %.1fs and retrying.", str(e), attempt, max_retries, delay)
                time.sleep(delay)
                delay *= 2
                continue
            logger.exception("Download failed after %d attempts: %s", max_retries, e)
            raise

# report helpers (using retry_spapi_call)
def create_orders_report(start: dt.datetime, end: dt.datetime) -> Dict[str, Any]:
    body = {
        "reportType": REPORT_TYPE,
        "dataStartTime": format_dt_z(start),
        "dataEndTime": format_dt_z(end),
        "marketplaceIds": [MARKETPLACE_ID],
    }
    logger.debug("Creating report for %s -> %s", body["dataStartTime"], body["dataEndTime"])
    return retry_spapi_call(lambda: spapi_request("POST", f"{REPORT_API_PREFIX}/reports", body=body))

def poll_report_until_done(report_id: str, timeout: int = MAX_REPORT_POLL_SECONDS) -> Dict[str, Any]:
    start = time.time()
    while True:
        resp = retry_spapi_call(lambda: spapi_request("GET", f"{REPORT_API_PREFIX}/reports/{report_id}", params={}))
        if "errors" in resp:
            raise RuntimeError(f"Error polling report {report_id}: {resp.get('errors')}")
        payload = resp.get("payload") or resp
        status = payload.get("processingStatus")
        logger.debug("Report %s status=%s", report_id, status)
        if status == "DONE":
            return payload
        if status in ("FATAL", "CANCELLED"):
            raise RuntimeError(f"Report {report_id} failed with status {status}: {payload}")
        if time.time() - start > timeout:
            raise TimeoutError(f"Polling report {report_id} timed out after {timeout}s")
        time.sleep(REPORT_POLL_INTERVAL)

def get_report_document(document_id: str) -> Dict[str, Any]:
    return retry_spapi_call(lambda: spapi_request("GET", f"{REPORT_API_PREFIX}/documents/{document_id}", params={}))

def download_report(url: str, compression: Optional[str]) -> str:
    logger.debug("Downloading report from %s (compression=%s)", url, compression)
    r = retry_requests_get(url, timeout=60)
    data = r.content
    if (compression or "").upper() == "GZIP":
        data = gzip.decompress(data)
    text = data.decode("utf-8", errors="replace")
    return text

# parsing
def parse_flat_orders(text: str, max_rows: Optional[int] = None) -> Tuple[List[Dict[str, str]], List[str]]:
    lines = text.splitlines()
    if not lines:
        return [], []
    reader = csv.reader(lines, delimiter="\t")
    raw_header = [h.strip() for h in next(reader, [])]
    norm_header = [normalize_col_name(h) for h in raw_header]

    rows = []
    for i, cols in enumerate(reader):
        if max_rows and i >= max_rows:
            break
        if not cols:
            continue
        mapped = {}
        for j, nh in enumerate(norm_header):
            mapped[nh] = cols[j] if j < len(cols) else ""
        mapped["_raw_row"] = cols
        rows.append(mapped)
    return rows, raw_header

# fee helpers (reuse existing)
def accumulate_unique_fee_items(rows: List[Dict[str, Any]]) -> List[Tuple[str, str, float]]:
    uniq = set()
    out = []
    for r in rows:
        sku = (r.get("sku") or "").strip()
        asin = (r.get("asin") or "").strip()
        price = None
        for cand in ("itemprice", "itempriceamount", "unitprice", "price", "itempriceamountcurrencyvalue"):
            v = r.get(cand)
            if not v:
                continue
            try:
                price = float(str(v).strip().replace("$", "").replace(",", ""))
                break
            except Exception:
                price = None
        if price is None:
            price = 0.0
        key = (sku, asin, price)
        if sku and asin and price > 0 and key not in uniq:
            uniq.add(key)
            out.append((sku, asin, price))
    return out

def estimate_fees_for_unique_items(unique_items: List[Tuple[str, str, float]]) -> Dict[Tuple[str, str, float], Any]:
    results: Dict[Tuple[str, str, float], Any] = {}
    for sku, asin, price in unique_items:
        conn = connect_database()
        cur = conn.cursor()
        try:
            db_entry = get_fee_estimate_from_product_mapping(cur, sku)
            if db_entry and db_entry.get("last_price") == price and db_entry.get("updated_at"):
                updated_at = db_entry["updated_at"]
                if isinstance(updated_at, dt.datetime) and updated_at.tzinfo is None:
                    updated_at = updated_at.replace(tzinfo=dt.timezone.utc)
                if isinstance(updated_at, dt.datetime) and (dt.datetime.now(dt.timezone.utc) - updated_at).days <= FEE_CACHE_TTL_DAYS:
                    results[(sku, asin, price)] = db_entry.get("fees")
                    continue
            try:
                fees = get_fees_estimate(sku, asin, price)
            except Exception as e:
                logger.exception("Fee estimate API error for %s/%s/%s: %s", sku, asin, price, e)
                fees = None
            if fees and isinstance(fees, dict) and "errors" not in fees:
                try:
                    upsert_fee_estimate_to_product_mapping(cur, sku, asin, price, fees)
                    conn.commit()
                except Exception:
                    conn.rollback()
                    logger.exception("Failed to upsert fee cache for %s", sku)
                results[(sku, asin, price)] = fees
            else:
                results[(sku, asin, price)] = fees
        finally:
            cur.close()
            conn.close()
    return results

# sanitizers & upsert helpers (same as earlier final version)
def _sanitize_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() in ("not available", "na", "n/a"):
        return None
    try:
        return float(s.replace("$", "").replace(",", ""))
    except Exception:
        return None

def _sanitize_int(v):
    if v is None:
        return None
    if isinstance(v, int):
        return v
    s = str(v).strip()
    if s == "" or s.lower() in ("not available", "na", "n/a"):
        return None
    try:
        return int(float(s))
    except Exception:
        return None

def _normalize_sql_datetime(val):
    if val is None:
        return None
    if isinstance(val, dt.datetime):
        return val.replace(tzinfo=None).isoformat(sep=" ")
    s = str(val).strip()
    if not s:
        return None
    if s.endswith("Z"):
        s = s[:-1]
    if re.search(r"[+\-]\d\d:\d\d$", s):
        s = re.sub(r"[+\-]\d\d:\d\d$", "", s)
    return s.replace("T", " ")

def upsert_order_items_bulk(conn: pyodbc.Connection, rows: List[Dict[str, Any]]):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE #BackfillOrderItems (
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
    );""")
    insert_sql = """
    INSERT INTO #BackfillOrderItems (
      OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
      SKU, ASIN, SSKU, Brand, Category, Title,
      Qty, UnitPrice, Subtotal, Currency,
      OrderStatus, LastUpdateDate,
      FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
      VAT, COG, Profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """
    params = []
    for r in rows:
        qty = _sanitize_int(r.get("Qty") or r.get("Quantity") or r.get("QuantityOrdered"))
        unit_price = _sanitize_float(r.get("UnitPrice") or r.get("SOLD") or r.get("item-price") or r.get("itemprice"))
        subtotal = _sanitize_float(r.get("Subtotal"))
        fee_incl = _sanitize_float(r.get("FeeIncl"))
        fee_pct = _sanitize_float(r.get("FeePct"))
        fba_fees_incl = _sanitize_float(r.get("FBAFeesIncl"))
        total_fee = _sanitize_float(r.get("TotalFee"))
        rvat = _sanitize_float(r.get("RVAT"))
        vat = _sanitize_float(r.get("VAT"))
        cog = _sanitize_float(r.get("COG"))
        profit = _sanitize_float(r.get("Profit"))
        order_date = _normalize_sql_datetime(r.get("OrderDate"))
        last_update = _normalize_sql_datetime(r.get("LastUpdateDate") or r.get("LastUpdate"))
        params.append((
            r.get("OrderItemKey"),
            r.get("AmazonOrderId"),
            r.get("OrderItemId"),
            order_date,
            r.get("SKU"),
            r.get("ASIN"),
            r.get("SSKU"),
            r.get("Brand"),
            r.get("Category"),
            r.get("Title"),
            qty,
            unit_price,
            subtotal,
            r.get("Currency"),
            r.get("OrderStatus"),
            last_update,
            fee_incl,
            fee_pct,
            fba_fees_incl,
            total_fee,
            rvat,
            vat,
            cog,
            profit,
        ))
    try:
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        cur.executemany(insert_sql, params)
    except Exception:
        logger.exception("Bulk insert into temp table failed. Example param (first): %s", params[0] if params else "<none>")
        raise
    merge_sql = """
    MERGE INTO spapi_app_user.OrderItems AS target
    USING #BackfillOrderItems AS src
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
    cur.execute("DROP TABLE #BackfillOrderItems")

# Lock helpers for backfill
def _acquire_lockfile(path: str, timeout_seconds: int = None) -> bool:
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        fd = os.open(path, flags)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        logger.info("Acquired lockfile: %s", path)
        return True
    except FileExistsError:
        if timeout_seconds:
            try:
                st = os.stat(path)
                age = time.time() - st.st_mtime
                if age > timeout_seconds:
                    logger.warning("Lockfile %s stale (age %.0f s). Removing.", path, age)
                    try:
                        os.remove(path)
                    except Exception:
                        logger.exception("Failed to remove stale lockfile %s", path)
                        return False
                    return _acquire_lockfile(path, timeout_seconds)
            except Exception:
                logger.exception("Error inspecting lockfile %s", path)
        return False
    except Exception:
        logger.exception("Error creating lockfile %s", path)
        return False

def acquire_backfill_lock() -> bool:
    return _acquire_lockfile(BACKFILL_LOCKFILE, LOCK_TIMEOUT_SECONDS)

def release_backfill_lock():
    try:
        if os.path.exists(BACKFILL_LOCKFILE):
            os.remove(BACKFILL_LOCKFILE)
            logger.info("Released backfill lock: %s", BACKFILL_LOCKFILE)
    except Exception:
        logger.exception("Failed to remove backfill lock on exit")

def wait_for_inventory_clear(max_wait: int) -> bool:
    if not os.path.exists(INVENTORY_LOCKFILE):
        return True
    logger.info("Inventory sync lock present (%s). Waiting up to %d seconds for it to clear...", INVENTORY_LOCKFILE, max_wait)
    start = time.time()
    while time.time() - start < max_wait:
        if not os.path.exists(INVENTORY_LOCKFILE):
            logger.info("Inventory lock cleared; proceeding with backfill.")
            return True
        time.sleep(5)
    logger.warning("Inventory sync lock still present after %d seconds; proceeding anyway (you may want to retry later).", max_wait)
    # Proceeding anyway is acceptable (we sanitize and use temp tables) but we warned.
    return True

# mapping from normalized row to internal shape
def build_order_item_from_flat_row(norm_row: Dict[str, str], raw_row: Optional[List[str]] = None) -> Optional[Dict[str,Any]]:
    def g(*candidates):
        for c in candidates:
            val = norm_row.get(normalize_col_name(c))
            if val not in (None, ""):
                return val
        return None
    amazon_order_id = g("amazon-order-id", "order-id", "orderid", "amazonorderid")
    if not amazon_order_id:
        return None
    order_item_id = g("order-item-id", "orderitemid", "orderitemcode")
    sku = g("seller-sku", "sku", "sellersku")
    asin = g("asin")
    qty_val = g("quantity", "quantityordered", "qty", "item-quantity")
    qty = 1
    if qty_val:
        try:
            qty = int(float(qty_val))
        except Exception:
            qty = 1
    price_val = None
    for cand in ("item-price", "itemprice", "itempriceamount", "unitprice", "price"):
        v = norm_row.get(normalize_col_name(cand))
        if v:
            try:
                price_val = float(str(v).strip().replace("$", "").replace(",", ""))
                break
            except Exception:
                price_val = None
    unit_price = price_val or 0.0
    currency = g("currency", "currencycode")
    purchase_date = g("purchase-date", "purchasedate")
    last_update_date = g("last-updated-date", "lastupdateddate", "lastupdated")
    title = g("product-name", "title", "productname")
    out = {
        "AmazonOrderId": amazon_order_id,
        "OrderItemId": order_item_id or None,
        "SKU": sku,
        "ASIN": asin,
        "Quantity": qty,
        "SOLD": unit_price if unit_price > 0 else None,
        "Currency": currency,
        "PurchaseDate": purchase_date,
        "LastUpdateDate": last_update_date,
        "Title": title,
    }
    return out

# process chunk (same logic as earlier improved version)
def process_chunk(start: dt.datetime, end: dt.datetime, max_rows: Optional[int], test_mode: bool=False) -> int:
    logger.info("Processing chunk %s -> %s", start.isoformat(), end.isoformat())
    create_resp = create_orders_report(start, end)
    report_id = (create_resp.get("payload") or {}).get("reportId") or create_resp.get("reportId")
    if not report_id:
        logger.error("No reportId returned: %s", create_resp)
        return 0
    logger.info("Created report %s", report_id)
    payload = poll_report_until_done(report_id)
    report_doc_id = payload.get("reportDocumentId")
    if not report_doc_id:
        logger.error("No reportDocumentId in payload: %s", payload)
        return 0
    doc_payload = get_report_document(report_doc_id)
    url = doc_payload.get("url")
    compression = doc_payload.get("compressionAlgorithm") or None
    if not url:
        logger.error("No download url for report document: %s", doc_payload)
        return 0
    text = download_report(url, compression)
    norm_rows, raw_header = parse_flat_orders(text, max_rows)
    logger.info("Report %s downloaded: raw header cols=%d rows=%d (max_rows=%s)", report_id, len(raw_header), len(norm_rows), str(max_rows))
    # debug dump
    debug_dump_path = os.path.join(LOG_DIR, f"backfill_report_{report_id}_debug.tsv")
    try:
        lines = text.splitlines()
        with open(debug_dump_path, "w", encoding="utf-8") as fh:
            if lines:
                fh.write(lines[0] + "\n")
                for ln in lines[1: min(1 + 50, len(lines))]:
                    fh.write(ln + "\n")
        logger.info("Wrote debug TSV to %s (raw header cols=%d, sample rows=%d)", debug_dump_path, len(raw_header), min(50, len(norm_rows)))
    except Exception:
        logger.exception("Failed to write debug TSV for report %s", report_id)
    parsed_items = []
    for norm_row in norm_rows:
        itm = build_order_item_from_flat_row(norm_row, raw_row=norm_row.get("_raw_row"))
        if itm:
            parsed_items.append(itm)
    logger.info("Parsed %d candidate order-items from flat report", len(parsed_items))
    if not parsed_items:
        return 0
    # enrichment
    conn = connect_database()
    cur = conn.cursor()
    try:
        all_skus = [i["SKU"] for i in parsed_items if i.get("SKU")]
        mapping = get_product_mapping(cur, all_skus) if all_skus else {}
        all_asins = list({m["asin"] for m in mapping.values() if m.get("asin")})
        details = get_product_details_by_asin(cur, all_asins) if all_asins else {}
    finally:
        cur.close()
        conn.close()
    missing_price_skus = [i["SKU"] for i in parsed_items if (not i.get("SOLD") or i.get("SOLD") == 0) and i.get("SKU")]
    fallback_prices = get_listing_prices_batch(missing_price_skus) if missing_price_skus else {}
    unique_items = accumulate_unique_fee_items(norm_rows if norm_rows else [])
    logger.info("Unique items for fee estimation: %d", len(unique_items))
    fee_map = estimate_fees_for_unique_items(unique_items) if unique_items else {}
    rows_to_upsert: List[Dict[str,Any]] = []
    for p in parsed_items:
        sku = p.get("SKU")
        asin = p.get("ASIN") or (mapping.get(sku) or {}).get("asin") if sku else p.get("ASIN")
        unit_price = p.get("SOLD") or fallback_prices.get(sku) or 0.0
        qty = p.get("Quantity") or 1
        subtotal = unit_price * qty if unit_price else None
        d = details.get(asin, {}) if asin else {}
        brand = d.get("brand") or "Not Available"
        category = d.get("category") or "Not Available"
        cog = parse_cost(d.get("cost")) if d.get("cost") else None
        fees = fee_map.get((sku, asin, unit_price))
        if fees and isinstance(fees, dict):
            ref_w = fees.get("ReferralFees", 0)
            fba_w = fees.get("FBAFees", 0)
            total_fee = -(ref_w + fba_w)
            fee_incl = -ref_w
            fba_fees_incl = -fba_w
            fee_pct = (ref_w / unit_price) * 100 if unit_price else None
            rv = (ref_w + fba_w) - (fees.get("ReferralFees", 0) + fees.get("FBAFees", 0))
        else:
            fee_incl = None
            fee_pct = None
            fba_fees_incl = None
            total_fee = None
            rv = None
        row = {
            "AmazonOrderId": p.get("AmazonOrderId"),
            "OrderItemId": p.get("OrderItemId"),
            "OrderDate": p.get("PurchaseDate"),
            "SKU": sku,
            "ASIN": asin,
            "SSKU": (mapping.get(sku) or {}).get("ssku") if sku else None,
            "Brand": brand,
            "Category": category,
            "Title": p.get("Title"),
            "Qty": qty,
            "UnitPrice": unit_price,
            "Subtotal": subtotal,
            "Currency": p.get("Currency"),
            "OrderStatus": None,
            "LastUpdateDate": p.get("LastUpdateDate"),
            "FeeIncl": fee_incl,
            "FeePct": fee_pct,
            "FBAFeesIncl": fba_fees_incl,
            "TotalFee": total_fee,
            "RVAT": rv,
            "VAT": None,
            "COG": (-cog) if cog is not None else None,
            "Profit": None,
        }
        order_item_id = row["OrderItemId"] or ""
        order_item_key = f"{row['AmazonOrderId']}:{order_item_id}" if order_item_id and order_item_id != "0" else f"{row['AmazonOrderId']}:0:{row.get('SKU') or ''}:{row.get('ASIN') or ''}"
        row["OrderItemKey"] = order_item_key
        rows_to_upsert.append(row)
    logger.info("Prepared %d rows to upsert", len(rows_to_upsert))
    if not rows_to_upsert:
        return 0
    # upsert in batches
    upsert_conn = pyodbc.connect(os.getenv("SQLSERVER_CONNECTION_STRING"))
    upsert_conn.autocommit = False
    inserted = 0
    try:
        for i in range(0, len(rows_to_upsert), BATCH_INSERT_SIZE):
            batch = rows_to_upsert[i : i + BATCH_INSERT_SIZE]
            logger.info("Upserting batch %d..%d", i+1, i+len(batch))
            try:
                upsert_order_items_bulk(upsert_conn, batch)
                upsert_conn.commit()
                inserted += len(batch)
            except Exception:
                upsert_conn.rollback()
                logger.exception("Bulk upsert failed; falling back to per-row upsert")
                cur = upsert_conn.cursor()
                try:
                    for r in batch:
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
                            r["AmazonOrderId"], r["OrderItemId"], r["OrderDate"],
                            r["SKU"], r["ASIN"], r["SSKU"], r["Brand"], r["Category"], r["Title"],
                            r["Qty"], r["UnitPrice"], r["Subtotal"], r["Currency"],
                            r["OrderStatus"], r["LastUpdateDate"],
                            r.get("FeeIncl"), r.get("FeePct"), r.get("FBAFeesIncl"), r.get("TotalFee"),
                            r.get("RVAT"), r.get("VAT"), r.get("COG"), r.get("Profit"),
                            r["OrderItemKey"]
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
                                r["OrderItemKey"], r["AmazonOrderId"], r["OrderItemId"], r["OrderDate"],
                                r["SKU"], r["ASIN"], r["SSKU"], r["Brand"], r["Category"], r["Title"],
                                r["Qty"], r["UnitPrice"], r["Subtotal"], r["Currency"],
                                r["OrderStatus"], r["LastUpdateDate"],
                                r.get("FeeIncl"), r.get("FeePct"), r.get("FBAFeesIncl"), r.get("TotalFee"),
                                r.get("RVAT"), r.get("VAT"), r.get("COG"), r.get("Profit"),
                            ))
                    upsert_conn.commit()
                    inserted += len(batch)
                except Exception:
                    upsert_conn.rollback()
                    logger.exception("Fallback per-row upsert completely failed for batch")
                    raise
                finally:
                    cur.close()
    finally:
        upsert_conn.close()
    logger.info("Upserted total %d rows for chunk %s->%s", inserted, start, end)
    return inserted

# CLI
def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="Start date (YYYY-MM-DD). Default: 365 days ago", default=None)
    p.add_argument("--end", help="End date (YYYY-MM-DD). Default: now", default=None)
    p.add_argument("--chunk-days", type=int, default=DEFAULT_CHUNK_DAYS, help="Days per chunk report")
    p.add_argument("--test-rows", type=int, default=0, help="If >0, limit rows parsed per report (quick test)")
    p.add_argument("--test-chunks", type=int, default=0, help="If >0, process only this many chunks and exit (quick test)")
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()

def main():
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")
    # Acquire backfill lock (single-process backfill)
    if not acquire_backfill_lock():
        logger.error("Could not acquire backfill lock (%s). Another backfill may be running. Exiting.", BACKFILL_LOCKFILE)
        return 1
    try:
        # Wait briefly if inventory syncing
        wait_for_inventory_clear(WAIT_FOR_INVENTORY_SECONDS)
        now = dt.datetime.now(dt.timezone.utc)
        if args.end:
            end = dt.datetime.fromisoformat(args.end)
            if end.tzinfo is None:
                end = end.replace(tzinfo=dt.timezone.utc)
        else:
            end = now
        if args.start:
            start = dt.datetime.fromisoformat(args.start)
            if start.tzinfo is None:
                start = start.replace(tzinfo=dt.timezone.utc)
        else:
            start = end - dt.timedelta(days=365)
        chunk_days = args.chunk_days
        cur_start = start
        total_inserted = 0
        chunks_processed = 0
        logger.info("Backfill window %s -> %s (chunk_days=%d)", start.isoformat(), end.isoformat(), chunk_days)
        while cur_start < end:
            cur_end = min(cur_start + dt.timedelta(days=chunk_days), end)
            inserted = process_chunk(cur_start, cur_end, max_rows=(args.test_rows or None), test_mode=(args.test_rows>0))
            total_inserted += inserted
            chunks_processed += 1
            if args.test_chunks and chunks_processed >= args.test_chunks:
                logger.info("Test chunk limit reached (%d). Stopping.", args.test_chunks)
                break
            cur_start = cur_end
            # small pause to avoid bursting report creation
            time.sleep(1.0)
        logger.info("Backfill complete. Chunks processed=%d total_rows_upserted=%d", chunks_processed, total_inserted)
        return 0
    finally:
        release_backfill_lock()

if __name__ == "__main__":
    main()