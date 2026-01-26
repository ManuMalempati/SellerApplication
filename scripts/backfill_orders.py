#!/usr/bin/env python3
r"""
backfill_orders.py

Backfill order items for a historical period using SP-API Reports (fast).

- Creates time-windowed reports (by default 7-day chunks) for the Orders "by last update" flat-file
  and downloads/parses each report rather than issuing N per-order API calls.
- Performs enrichment locally:
  * resolves SKU -> ASIN/SSKU mapping from ProductMapping
  * looks up product details by ASIN (cost/brand/category)
  * fetches fallback listing prices (batched) where needed
  * estimates fees for unique (sku,asin,price) using existing fee-estimate & cache logic
- Upserts results into spapi_app_user.OrderItems using a temp-table + MERGE (bulk), with fallback to per-row upsert.
- Includes verbose debug/logging and an easy "small test" mode.

Usage (examples)
 - Dry run / small test for 1 day:
     python scripts/backfill_orders.py --start 2025-12-01 --end 2025-12-02 --chunk-days 1 --test-rows 200
 - Full 1-year backfill (default behavior will backfill 365 days ending today if no args passed):
     python scripts/backfill_orders.py

Notes
 - Requires the same environment (.env) and DB config as the rest of the app.
 - Tested to be non-destructive: upserts are idempotent.
"""
from __future__ import annotations
import argparse
import sys
import os
import time
import datetime as dt
import csv
import gzip
import io
import logging
import requests
from typing import List, Dict, Tuple, Any, Optional
from dotenv import load_dotenv

# ensure repo root on sys.path (assumes this script lives in gcinventory/scripts)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

# load environment
ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# app imports (reuse logic where possible)
from app.auth import spapi_request  # SP-API helper
from app.database import (
    connect_database,
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    upsert_fee_estimate_to_product_mapping,
    get_fee_estimate_from_product_mapping,
)
from app.estimates import get_fees_estimate
from app.orders import get_listing_prices_batch  # reuse pricing batch function
import json

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
REPORT_POLL_INTERVAL = float(os.getenv("REPORT_POLL_INTERVAL", "5"))  # seconds between polls
BATCH_INSERT_SIZE = int(os.getenv("BACKFILL_BATCH_SIZE", "1000"))

# Helpers for canonical UTC Z formatting
def format_dt_z(d: dt.datetime) -> str:
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- SP-API Reports helpers ---
def create_orders_report(start: dt.datetime, end: dt.datetime) -> Dict[str, Any]:
    body = {
        "reportType": REPORT_TYPE,
        "dataStartTime": format_dt_z(start),
        "dataEndTime": format_dt_z(end),
        "marketplaceIds": [MARKETPLACE_ID],
    }
    logger.debug("Creating report for %s -> %s", body["dataStartTime"], body["dataEndTime"])
    return spapi_request("POST", f"{REPORT_API_PREFIX}/reports", body=body)

def poll_report_until_done(report_id: str, timeout: int = MAX_REPORT_POLL_SECONDS) -> Dict[str, Any]:
    start = time.time()
    while True:
        resp = spapi_request("GET", f"{REPORT_API_PREFIX}/reports/{report_id}", params={})
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
    resp = spapi_request("GET", f"{REPORT_API_PREFIX}/documents/{document_id}", params={})
    if "errors" in resp:
        raise RuntimeError(f"Error fetching report document {document_id}: {resp.get('errors')}")
    return resp.get("payload") or resp

def download_report(url: str, compression: Optional[str]) -> str:
    logger.debug("Downloading report from %s (compression=%s)", url, compression)
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.content
    if (compression or "").upper() == "GZIP":
        data = gzip.decompress(data)
    # decode as UTF-8, tolerant
    text = data.decode("utf-8", errors="replace")
    return text

# --- Parsing helper (flat file is typically TAB-delimited) ---
def parse_flat_orders(text: str, max_rows: Optional[int] = None) -> Tuple[List[Dict[str, str]], List[str]]:
    lines = text.splitlines()
    if not lines:
        return [], []
    reader = csv.reader(lines, delimiter="\t")
    header = [h.strip() for h in next(reader, [])]
    rows = []
    for i, row in enumerate(reader):
        if max_rows and i >= max_rows:
            break
        if not row:
            continue
        # map column name -> value
        mapped = {header[j]: row[j] if j < len(row) else "" for j in range(len(header))}
        rows.append(mapped)
    return rows, header

# --- Enrichment helpers (reuse DB cache and fee-estimate) ---
def accumulate_unique_fee_items(rows: List[Dict[str, Any]]) -> List[Tuple[str, str, float]]:
    uniq = set()
    out = []
    for r in rows:
        sku = (r.get("SellerSKU") or r.get("sku") or r.get("SKU") or "").strip()
        asin = (r.get("ASIN") or r.get("asin") or "").strip()
        # try to get unit price from known columns
        price = None
        for cand in ("ItemPrice", "ItemPriceAmount", "UnitPrice", "ItemPriceAmountCurrencyValue"):
            # handle nested shapes or simple strings
            v = r.get(cand)
            if not v:
                continue
            try:
                price = float(v)
                break
            except Exception:
                try:
                    price = float(str(v).strip())
                    break
                except Exception:
                    price = None
        # fallback to 'Price' fields in flat report (various names)
        if price is None:
            # typical order flat-file columns include 'Item Price' or 'ItemPrice'
            for key in r.keys():
                if key.lower().replace(" ", "") in ("itemprice", "unitprice", "price"):
                    try:
                        price = float(r.get(key) or 0)
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

# Fee estimation wrapper using same caching semantics as app.orders.estimate_fees_for_item
def estimate_fees_for_unique_items(unique_items: List[Tuple[str,str,float]]) -> Dict[Tuple[str,str,float], Any]:
    cache: Dict[Tuple[str,str,float], Any] = {}
    results: Dict[Tuple[str,str,float], Any] = {}
    for sku, asin, price in unique_items:
        # check DB cache first
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
            # call SP-API fees estimate
            try:
                fees = get_fees_estimate(sku, asin, price)
            except Exception as e:
                logger.exception("Fee estimate API error for %s/%s/%s: %s", sku, asin, price, e)
                fees = None
            if fees and isinstance(fees, dict) and "errors" not in fees:
                # persist into ProductMapping
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

# --- Upsert helpers (bulk MERGE into OrderItems) ---
import pyodbc
def upsert_order_items_bulk(conn: pyodbc.Connection, rows: List[Dict[str, Any]]):
    cur = conn.cursor()
    # create local temp table
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
        params.append((
            r.get("OrderItemKey"),
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
    cur.executemany(insert_sql, params)
    # MERGE into target table
    cur.execute("""
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
    """)
    cur.execute("DROP TABLE #BackfillOrderItems")

# --- Core backfill flow ---
def build_order_item_from_flat_row(row: Dict[str,str]) -> Optional[Dict[str,Any]]:
    """
    Map typical flat-file columns to the internal order-item shape we upsert.
    Be defensive about column names (different marketplaces/report variations).
    """
    # common column names (variants)
    amazon_order_id = row.get("AmazonOrderId") or row.get("order-id") or row.get("Order ID") or row.get("OrderID")
    if not amazon_order_id:
        # if no order id, skip
        return None

    order_item_id = row.get("OrderItemCode") or row.get("order-item-id") or row.get("OrderItemId") or row.get("OrderItemID") or ""
    sku = (row.get("SellerSKU") or row.get("seller-sku") or row.get("Seller-SKU") or row.get("SKU") or "").strip() or None
    asin = (row.get("ASIN") or row.get("asin") or "").strip() or None

    # parse qty and price defensively
    qty = 1
    for key in ("QuantityOrdered", "quantity", "quantityordered", "qty", "TotalQuantity"):
        if key in row and row[key]:
            try:
                qty = int(float(row[key]))
                break
            except Exception:
                pass

    # price - try several column names
    unit_price = None
    for key in ("ItemPrice", "ItemPriceAmount", "Item Price", "ItemPriceAmountCurrencyValue", "Price"):
        v = row.get(key)
        if v:
            try:
                unit_price = float(v)
                break
            except Exception:
                try:
                    unit_price = float(v.replace("$","").replace(",",""))
                    break
                except Exception:
                    unit_price = None

    # currency
    currency = row.get("Currency") or row.get("CurrencyCode") or row.get("Currency Code") or ""

    purchase_date = row.get("PurchaseDate") or row.get("purchase-date") or row.get("Purchase Date") or ""
    last_update_date = row.get("LastUpdateDate") or row.get("Last Updated") or purchase_date

    title = row.get("Title") or row.get("product-name") or row.get("Product Name") or None

    out = {
        "AmazonOrderId": amazon_order_id,
        "OrderItemId": order_item_id or None,
        "SKU": sku,
        "ASIN": asin,
        "Quantity": qty,
        "SOLD": unit_price,
        "Currency": currency,
        "PurchaseDate": purchase_date,
        "LastUpdateDate": last_update_date,
        "Title": title,
    }
    return out

def process_chunk(start: dt.datetime, end: dt.datetime, max_rows: Optional[int], test_mode: bool=False) -> int:
    """
    Create report, download, parse, enrich and upsert one time chunk.
    Returns number of rows upserted.
    """
    logger.info("Processing chunk %s -> %s", start.isoformat(), end.isoformat())
    create_resp = create_orders_report(start, end)
    # support both {"payload":{"reportId":...}} and {"reportId":...}
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
    flat_rows, header = parse_flat_orders(text, max_rows)
    logger.info("Report %s downloaded: header cols=%d rows=%d (max_rows=%s)", report_id, len(header), len(flat_rows), str(max_rows))

    # transform flat rows into our internal item form
    parsed_items = []
    for fr in flat_rows:
        itm = build_order_item_from_flat_row(fr)
        if itm:
            parsed_items.append(itm)
    logger.info("Parsed %d candidate order-items from flat report", len(parsed_items))

    if not parsed_items:
        return 0

    # Enrichment: mapping, details, fallback pricing, fees
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

    # Determine missing prices to fetch in batches
    missing_price_skus = [i["SKU"] for i in parsed_items if (not i.get("SOLD") or i.get("SOLD") == 0) and i.get("SKU")]
    fallback_prices = get_listing_prices_batch(missing_price_skus) if missing_price_skus else {}

    # Prepare for fee estimates
    unique_items = accumulate_unique_fee_items(parsed_items)
    logger.info("Unique items for fee estimation: %d", len(unique_items))
    fee_map = estimate_fees_for_unique_items(unique_items) if unique_items else {}

    # Build upsert rows
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

        # fees lookup
        fees = fee_map.get((sku, asin, unit_price))
        if fees and isinstance(fees, dict):
            ref_w = fees.get("ReferralFees", 0)
            fba_w = fees.get("FBAFees", 0)
            total_fee = -(ref_w + fba_w)
            fee_incl = -ref_w
            fba_fees_incl = -fba_w
            fee_pct = (ref_w / unit_price) * 100 if unit_price else None
            rv = (ref_w + fba_w) - (fees.get("ReferralFees",0) + fees.get("FBAFees",0))  # estimator's R.VAT shape
        else:
            fee_incl = "Not Available"
            fee_pct = "Not Available"
            fba_fees_incl = "Not Available"
            total_fee = "Not Available"
            rv = "Not Available"

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
            "FeeIncl": fee_incl if fee_incl != None else None,
            "FeePct": fee_pct if fee_pct != None else None,
            "FBAFeesIncl": fba_fees_incl if fba_fees_incl != None else None,
            "TotalFee": total_fee if total_fee != None else None,
            "RVAT": rv if rv != None else None,
            "VAT": None,
            "COG": (-cog) if cog is not None else None,
            "Profit": None,
        }
        # Compute OrderItemKey (same as other scripts)
        order_item_id = row["OrderItemId"] or ""
        order_item_key = f"{row['AmazonOrderId']}:{order_item_id}" if order_item_id and order_item_id != "0" else f"{row['AmazonOrderId']}:0:{row.get('SKU') or ''}:{row.get('ASIN') or ''}"
        row["OrderItemKey"] = order_item_key
        rows_to_upsert.append(row)

    logger.info("Prepared %d rows to upsert", len(rows_to_upsert))
    if not rows_to_upsert:
        return 0

    # Bulk upsert in batches
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
                logger.exception("Bulk upsert failed for batch; falling back to per-row upsert")
                cur = upsert_conn.cursor()
                try:
                    for r in batch:
                        # simple per-row upsert (same SQL as sync script)
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

# --- CLI / orchestration ---
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

    # compute date window
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
        # small pause to avoid bursting reports
        time.sleep(1.0)

    logger.info("Backfill complete. Chunks processed=%d total_rows_upserted=%d", chunks_processed, total_inserted)

if __name__ == "__main__":
    main()