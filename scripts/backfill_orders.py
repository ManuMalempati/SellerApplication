#!/usr/bin/env python3
"""
backfill_orders.py

One-off backfill for Orders over a historical period using SP-API Reports.

Minor updates:
 - Use live fee estimates (no persistent DB fee cache).
 - Log raw fee payloads at DEBUG to help diagnose missing RVAT/VAT.
 - Keep existing retry/backoff for SP-API; add a small debug line when we call fee API.
 - Do not make large structural changes; keep semantics intact.
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
    # fee cache stubs exist in app.database; we won't rely on them
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

# fee helpers (live fetch; no persistent cache)
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
        logger.debug("Estimating fees (live) for SKU=%s ASIN=%s price=%.2f", sku, asin, price)
        try:
            fees = get_fees_estimate(sku, asin, price)
            logger.debug("Raw fees for %s/%s@%s: %s", sku, asin, price, fees)
        except Exception as e:
            logger.exception("Fee estimate API error for %s/%s/%s: %s", sku, asin, price, e)
            fees = None
        results[(sku, asin, price)] = fees
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
        if params:
            logger.debug("Inserting %d backfill rows. Example param[0]: %r", len(params), params[0])
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
            logger.info("Released backfill lock")
    except Exception:
        logger.exception("Failed to remove backfill lock")