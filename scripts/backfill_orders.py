#!/usr/bin/env python3
"""
backfill_orders.py

One-off backfill for Orders over a historical period using SP-API Reports.

Key features in this updated file:
 - Rotating file logging (safer on Windows) with console fallback.
 - Live fee estimates (no persistent DB fee cache). Honours SKIP_FEE_ESTIMATION env if set,
   but default behavior is to perform fee estimation.
 - Robust bulk upsert: sanitizes numeric params, falls back to per-row insert with coercion,
   and skips bad rows while logging details.
 - Rate-limit aware report creation/polling and retries.
 - Minimal behavior changes from original: chunking, report flow, parsing, and upsert retained.
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
import math
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
REQUIRED_ENVS = [
    "LWA_TOKEN_URL",
    "LWA_CLIENT_ID",
    "LWA_CLIENT_SECRET",
    "LWA_REFRESH_TOKEN",
    "SPAPI_ENDPOINT",
    "MARKETPLACE_ID",
    "SQLSERVER_CONNECTION_STRING",
]
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
    # fee cache stubs exist in app.database but are not used
    upsert_fee_estimate_to_product_mapping,
    get_fee_estimate_from_product_mapping,
)
from app.estimates import get_fees_estimate
from app.orders import get_listing_prices_batch
import json
import pyodbc
from logging.handlers import RotatingFileHandler

# --------------------------------------------------------------------
# Logging (Rotating + Console)
# --------------------------------------------------------------------
LOG_DIR = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)

logger = logging.getLogger("backfill_orders")
logger.setLevel(logging.INFO)

# Console handler (always enabled)
console = logging.StreamHandler()
console.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
logger.addHandler(console)

# Rotating file handler (safe, no locking issues)
log_path = os.path.join(LOG_DIR, "backfill_orders.log")
try:
    fh = RotatingFileHandler(
        log_path,
        maxBytes=5 * 1024 * 1024,   # 5 MB per file
        backupCount=10,             # keep 10 backups
        encoding="utf-8"
    )
    fh.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    logger.addHandler(fh)
except PermissionError:
    logger.warning("Permission denied opening %s — using console logging only", log_path)
except Exception:
    logger.exception("Unexpected error creating rotating log handler")

# --------------------------------------------------------------------
# Config
# --------------------------------------------------------------------
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

BACKFILL_LOCKFILE = os.path.join(REPO_ROOT, "backfill.lock")
INVENTORY_LOCKFILE = os.path.join(REPO_ROOT, "inventorysync.lock")
WAIT_FOR_INVENTORY_SECONDS = int(
    os.getenv("BACKFILL_WAIT_FOR_INVENTORY_SECONDS", str(10 * 60))
)  # default 10 minutes
LOCK_TIMEOUT_SECONDS = int(os.getenv("BACKFILL_LOCK_TIMEOUT_SECONDS", str(24 * 3600)))

# Honor SKIP_FEE_ESTIMATION env var if explicitly set to true-ish (default = False)
SKIP_FEE_ESTIMATION = str(os.getenv("SKIP_FEE_ESTIMATION", "")).lower() in ("1", "true", "yes")

# small helpers
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


# --------------------------------------------------------------------
# Retry wrappers
# --------------------------------------------------------------------
def retry_spapi_call(
    fn: Callable[..., Dict[str, Any]],
    *args,
    max_retries: int = MAX_RETRIES,
    initial_delay: float = INITIAL_RETRY_DELAY,
    **kwargs,
) -> Dict[str, Any]:
    delay = initial_delay
    for attempt in range(1, max_retries + 1):
        resp = fn(*args, **kwargs)
        if not isinstance(resp, dict) or "errors" not in resp:
            return resp
        err_codes = [e.get("code") for e in resp.get("errors", []) if isinstance(e, dict)]
        if "QuotaExceeded" in err_codes or "RequestThrottled" in err_codes or "TooManyRequests" in err_codes:
            if attempt < max_retries:
                logger.warning(
                    "SP-API rate limit '%s' hit (attempt %d/%d). Backing off %.1fs and retrying.",
                    err_codes,
                    attempt,
                    max_retries,
                    delay,
                )
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


# --------------------------------------------------------------------
# Report helpers
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# Parsing helpers
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# Fee helpers (live fetch)
# --------------------------------------------------------------------
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
    if SKIP_FEE_ESTIMATION:
        logger.info("SKIP_FEE_ESTIMATION enabled: skipping fee API calls for %d unique items", len(unique_items))
        for key in unique_items:
            results[key] = None
        return results

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


# --------------------------------------------------------------------
# Sanitizers & upsert helpers
# --------------------------------------------------------------------
def _sanitize_float(v):
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip()
    if s == "" or s.lower() in ("not available", "na", "n/a"):
        return None
    try:
        return float(s.replace("$", "").replace(",", "").replace("AED", "").strip())
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


def _build_params_from_row(r: Dict[str, Any]):
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
    return (
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
    )


def upsert_order_items_bulk(conn: pyodbc.Connection, rows: List[Dict[str, Any]]):
    cur = conn.cursor()
    cur.execute(
        """
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
    );"""
    )
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
        params.append(_build_params_from_row(r))

    # Try bulk insert; if it fails, fall back to robust per-row insertion
    try:
        try:
            cur.fast_executemany = True
        except Exception:
            pass
        if params:
            logger.debug("Inserting %d backfill rows (bulk). Example param[0]: %r", len(params), params[0])
        cur.executemany(insert_sql, params)
    except Exception:
        logger.exception(
            "Bulk insert into temp table failed. Falling back to per-row insert. Example param (first): %s",
            params[0] if params else "<none>",
        )
        # Per-row fallback with stricter sanitization and skipping bad rows
        inserted = 0
        for idx, r in enumerate(rows):
            p = _build_params_from_row(r)
            try:
                cur.execute(insert_sql, p)
                inserted += 1
            except Exception as e:
                logger.warning(
                    "Per-row insert failed for OrderItemKey=%s (row %d). Attempting coercion. Error: %s",
                    r.get("OrderItemKey"),
                    idx,
                    e,
                )
                # Attempt to coerce string numeric values aggressively
                coerced = list(p)
                numeric_indices = (10, 11, 12, 16, 17, 18, 19, 20, 21, 22, 23)
                for i_v in numeric_indices:
                    try:
                        val = coerced[i_v]
                        if val in (None, "", "Not Available"):
                            coerced[i_v] = None
                        else:
                            coerced[i_v] = float(str(val).replace("$", "").replace(",", "").replace("AED", "").strip())
                    except Exception:
                        coerced[i_v] = None
                try:
                    cur.execute(insert_sql, tuple(coerced))
                    inserted += 1
                except Exception as e2:
                    logger.error("Row still failed after coercion. Skipping OrderItemKey=%s. Error: %s", r.get("OrderItemKey"), e2)
                    continue
        logger.info("Per-row insert completed; rows staged: %d (attempted %d)", inserted, len(rows))

    # Merge into target and drop staging table
    try:
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
    finally:
        try:
            cur.execute("DROP TABLE #BackfillOrderItems")
        except Exception:
            pass


# --------------------------------------------------------------------
# Locks & helpers
# --------------------------------------------------------------------
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


# --------------------------------------------------------------------
# Chunk processing and main driver (keeps original logic and flow)
# --------------------------------------------------------------------
def build_order_item_from_flat_row(norm_row: Dict[str, str], raw_row: Optional[List[str]] = None) -> Optional[Dict[str, Any]]:
    """
    Map normalized flat-file row -> minimal order-item dict used for enrichment.
    This mirrors previous mapping logic but is tolerant of missing fields.
    """
    # helper to pick first non-empty candidate
    def g(*cands):
        for c in cands:
            v = norm_row.get(c)
            if v is not None and str(v).strip() != "":
                return v
        return None

    sku = g("sku")
    asin = g("asin")
    qty = g("quantity")
    currency = g("currency")
    item_price = g("itemprice", "item-price", "itempriceamount")
    purchase_date = g("purchaseDate", "purchase-date")
    last_updated = g("lastupdateddate", "last-updated-date")
    order_status = g("orderstatus", "order-status")

    if not sku or not asin:
        return None

    try:
        qty_i = int(float(qty)) if qty else 1
    except Exception:
        qty_i = 1

    try:
        price_f = float(str(item_price).replace("$", "").replace(",", "")) if item_price else 0.0
    except Exception:
        price_f = 0.0

    return {
        "sku": sku,
        "asin": asin,
        "quantity": qty_i,
        "UnitPrice": price_f,
        "Currency": currency or None,
        "OrderDate": purchase_date,
        "LastUpdate": last_updated,
        "OrderStatus": order_status,
        "_raw_row": raw_row,
    }


def process_chunk(start: dt.datetime, end: dt.datetime, max_rows: Optional[int], test_mode: bool = False) -> int:
    """
    Process a single chunk window [start, end). Returns number of rows upserted.
    """
    logger.info("Processing chunk %s -> %s", start.isoformat(), end.isoformat())
    # 1) Create report
    create_resp = create_orders_report(start, end)
    if "errors" in create_resp:
        logger.error("create_orders_report error: %s", create_resp.get("errors"))
        return 0

    report_id = (create_resp.get("payload") or {}).get("reportId") or create_resp.get("reportId")
    if not report_id:
        logger.error("No reportId returned: %s", create_resp)
        return 0
    logger.info("Created report %s", report_id)

    # 2) Poll until done
    doc_payload = poll_report_until_done(report_id)
    doc_id = doc_payload.get("reportDocumentId")
    if not doc_id:
        logger.error("No reportDocumentId for report %s", report_id)
        return 0

    # 3) Get report document
    doc = get_report_document(doc_id)
    url = (doc.get("payload") or doc).get("url") or doc.get("url")
    compression = (doc.get("payload") or doc).get("compressionAlgorithm") or doc.get("compressionAlgorithm")
    if not url:
        logger.error("No URL for report document %s", doc_id)
        return 0

    text = download_report(url, compression)
    rows, header = parse_flat_orders(text, max_rows=max_rows)

    if not rows:
        logger.info("Parsed 0 candidate order-items from flat report")
        return 0

    # optional debug TSV output for inspection
    debug_tsv_path = os.path.join(LOG_DIR, f"backfill_report_{report_id}_debug.tsv")
    try:
        with open(debug_tsv_path, "w", encoding="utf-8", newline="") as fh:
            writer = csv.writer(fh, delimiter="\t")
            writer.writerow(header)
            for r in rows[:200]:
                writer.writerow(r.get("_raw_row", []))
        logger.info("Wrote debug TSV to %s (raw header cols=%d, sample rows=%d)", debug_tsv_path, len(header), min(len(rows), 200))
    except Exception:
        logger.exception("Failed to write debug TSV for report %s", report_id)

    # Build candidate order-items and unique fee items
    candidates = []
    for r in rows:
        item = build_order_item_from_flat_row(r, r.get("_raw_row"))
        if item:
            candidates.append(item)

    if not candidates:
        logger.info("Parsed 0 candidate order-items from flat report")
        return 0

    logger.info("Parsed %d candidate order-items from flat report", len(candidates))

    unique_fee_items = accumulate_unique_fee_items(candidates)
    logger.info("Unique items for fee estimation: %d", len(unique_fee_items))

    fees_map = estimate_fees_for_unique_items(unique_fee_items)

    # Enrich rows with product mapping/details and fees
    # Build SKU list for mapping
    skus = list({c["sku"] for c in candidates if c.get("sku")})
    conn = connect_database()
    cur = conn.cursor()
    try:
        mapping = get_product_mapping(cur, skus) if skus else {}
    finally:
        cur.close()
        conn.close()

    asins = list({m.get("asin") for m in mapping.values() if m.get("asin")})
    conn2 = connect_database()
    cur2 = conn2.cursor()
    try:
        details = get_product_details_by_asin(cur2, asins) if asins else {}
    finally:
        cur2.close()
        conn2.close()

    # Prepare upsert rows
    prepared_rows: List[Dict[str, Any]] = []
    for c in candidates:
        sku = c.get("sku")
        asin = c.get("asin")
        m = mapping.get(sku, {})
        d = details.get(asin, {})

        unit_price = c.get("UnitPrice") or 0.0
        qty = c.get("quantity") or 1

        fee_key = (sku, asin, unit_price)
        fees = fees_map.get(fee_key)

        # parse fees payload into referral and fba amounts (best-effort)
        ref_amt = None
        fba_amt = None
        if isinstance(fees, dict):
            ref_amt = _safe_val = None
            try:
                ref_amt = _safe_val = (fees.get("ReferralFees") or fees.get("ReferralFee") or fees.get("ReferralFee.Amount"))
                fba_amt = fees.get("FBAFees") or fees.get("FBAFee")
            except Exception:
                ref_amt = None
                fba_amt = None

        # VAT calculation (per-unit)
        try:
            GOVT_VAT_RATE_DIVISOR = float(os.getenv("GOVT_VAT_RATE_DIVISOR", "21"))
            GOVT_VAT_RATE = 1.0 / GOVT_VAT_RATE_DIVISOR
        except Exception:
            GOVT_VAT_RATE = 1.0 / 21.0

        vat_amt = unit_price * GOVT_VAT_RATE if unit_price else None

        # RVAT: VAT on fees - use FEES_ESTIMATE_VAT_MULTIPLIER if provided
        try:
            FEES_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
        except Exception:
            FEES_VAT_MULTIPLIER = 1.0

        ref_vat = None
        fba_vat = None
        if ref_amt is not None:
            try:
                ref_amt_f = float(ref_amt)
                if FEES_VAT_MULTIPLIER > 1.0:
                    ref_vat = ref_amt_f * (1.0 - 1.0 / FEES_VAT_MULTIPLIER)
                else:
                    ref_vat = ref_amt_f * GOVT_VAT_RATE
            except Exception:
                ref_vat = None
        if fba_amt is not None:
            try:
                fba_amt_f = float(fba_amt)
                if FEES_VAT_MULTIPLIER > 1.0:
                    fba_vat = fba_amt_f * (1.0 - 1.0 / FEES_VAT_MULTIPLIER)
                else:
                    fba_vat = fba_amt_f * GOVT_VAT_RATE
            except Exception:
                fba_vat = None

        total_rvat = None
        if ref_vat is not None or fba_vat is not None:
            total_rvat = (ref_vat or 0.0) + (fba_vat or 0.0)

        prepared = {
            "OrderItemKey": f"{c.get('sku')}_{c.get('asin')}_{c.get('_raw_row', [''])[0]}_{int(time.time())}",
            "AmazonOrderId": None,
            "OrderItemId": None,
            "OrderDate": c.get("OrderDate"),
            "SKU": sku,
            "ASIN": asin,
            "SSKU": m.get("ssku") if m else None,
            "Brand": d.get("brand") if d else None,
            "Category": d.get("category") if d else None,
            "Title": d.get("item_name") or d.get("title") or None,
            "Qty": qty,
            "UnitPrice": unit_price,
            "Subtotal": unit_price * qty if unit_price else None,
            "Currency": c.get("Currency"),
            "OrderStatus": c.get("OrderStatus"),
            "LastUpdateDate": c.get("LastUpdate"),
            "FeeIncl": (-float(ref_amt)) if ref_amt is not None else None,
            "FeePct": (float(ref_amt) / unit_price * 100.0) if (ref_amt is not None and unit_price) else None,
            "FBAFeesIncl": (-float(fba_amt)) if fba_amt is not None else None,
            "TotalFee": (-(float(ref_amt or 0.0) + float(fba_amt or 0.0))) if (ref_amt is not None or fba_amt is not None) else None,
            "RVAT": (-total_rvat) if total_rvat is not None else None,
            "VAT": (-vat_amt) if vat_amt is not None else None,
            "COG": (-parse_cost(d.get("cost"))) if d and d.get("cost") else None,
            "Profit": None,
        }
        prepared_rows.append(prepared)

    # Upsert in batches
    total_upserted = 0
    conn_upsert = connect_database()
    try:
        for i in range(0, len(prepared_rows), BATCH_INSERT_SIZE):
            batch = prepared_rows[i : i + BATCH_INSERT_SIZE]
            logger.info("Prepared %d rows to upsert", len(batch))
            logger.info("Upserting batch %d..%d", i + 1, i + len(batch))
            try:
                upsert_order_items_bulk(conn_upsert, batch)
                conn_upsert.commit()
                total_upserted += len(batch)
                logger.info("Upserted total %d rows for chunk %s->%s", len(batch), start.isoformat(), end.isoformat())
            except Exception:
                logger.exception("Bulk upsert failed for batch; attempting per-row fallback")
                try:
                    conn_upsert.rollback()
                except Exception:
                    logger.exception("Rollback failed")
                cur = conn_upsert.cursor()
                upserted_row_count = 0
                for r in batch:
                    params = _build_params_from_row(r)
                    try:
                        # per-row insert via staging MERGE approach in original script:
                        # Here we insert row directly into target to avoid repeated temp table creation.
                        sql_insert = """
                        INSERT INTO spapi_app_user.OrderItems (
                            OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
                            SKU, ASIN, SSKU, Brand, Category, Title,
                            Qty, UnitPrice, Subtotal, Currency,
                            OrderStatus, LastUpdateDate,
                            FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
                            VAT, COG, Profit, FirstSeenAt, LastSeenAt
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, SYSUTCDATETIME(), SYSUTCDATETIME())
                        """
                        cur.execute(sql_insert, (
                            params[0], params[1], params[2], params[3],
                            params[4], params[5], params[6], params[7], params[8], params[9],
                            params[10], params[11], params[12], params[13],
                            params[14], params[15],
                            params[16], params[17], params[18], params[19], params[20],
                            params[21], params[22], params[23],
                        ))
                        upserted_row_count += 1
                    except Exception as e:
                        logger.warning("Per-row insert failed for OrderItemKey=%s: %s", r.get("OrderItemKey"), e)
                        # skip problematic row
                        continue
                try:
                    conn_upsert.commit()
                except Exception:
                    conn_upsert.rollback()
                total_upserted += upserted_row_count
                logger.info("Per-row fallback upserted %d rows for this batch", upserted_row_count)
    finally:
        conn_upsert.close()

    logger.info("Chunk finished: %s -> %s ; upserted %d rows", start.isoformat(), end.isoformat(), total_upserted)
    return total_upserted


def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--start", help="Start date (YYYY-MM-DD)", required=True)
    p.add_argument("--end", help="End date (YYYY-MM-DD)", required=True)
    p.add_argument("--max-rows", type=int, default=None)
    p.add_argument("--verbose", action="store_true")
    return p.parse_args()


def main():
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)
        logger.debug("Verbose logging enabled")

    try:
        start = dt.datetime.fromisoformat(args.start).replace(tzinfo=dt.timezone.utc)
        end = dt.datetime.fromisoformat(args.end).replace(tzinfo=dt.timezone.utc)
    except Exception:
        logger.error("Invalid start/end date. Use YYYY-MM-DD")
        return 2

    chunk_days = DEFAULT_CHUNK_DAYS
    total_rows = 0
    chunk_start = start
    if not acquire_backfill_lock():
        logger.info("Another backfill is running or lockfile exists. Exiting.")
        return 1
    try:
        while chunk_start < end:
            chunk_end = min(chunk_start + dt.timedelta(days=chunk_days), end)
            # If inventorysync lock exists, wait a bit then skip this run if still present
            waited = 0
            while os.path.exists(INVENTORY_LOCKFILE) and waited < WAIT_FOR_INVENTORY_SECONDS:
                logger.info("inventorysync.lock present; waiting up to %d seconds for it to clear...", WAIT_FOR_INVENTORY_SECONDS)
                time.sleep(5)
                waited += 5
            if os.path.exists(INVENTORY_LOCKFILE):
                logger.warning("inventorysync.lock still present after wait; skipping chunk %s -> %s", chunk_start.isoformat(), chunk_end.isoformat())
                chunk_start = chunk_end
                continue

            rows_upserted = process_chunk(chunk_start, chunk_end, args.max_rows, test_mode=False)
            total_rows += rows_upserted
            chunk_start = chunk_end
    finally:
        release_backfill_lock()

    logger.info("Backfill complete. Chunks processed up to %s total_rows_upserted=%d", end.isoformat(), total_rows)
    return 0


if __name__ == "__main__":
    rc = main()
    sys.exit(int(rc or 0))