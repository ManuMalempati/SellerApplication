#!/usr/bin/env python3
import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
import threading
import csv
import io

from .auth import spapi_request
from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    connect_database,
)
from .estimates import get_fees_estimate

# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------

GOVT_VAT_RATE = (
    1 / float(os.getenv("GOVT_VAT_RATE_DIVISOR", "1"))
    if os.getenv("GOVT_VAT_RATE_DIVISOR")
    else 0.0
)

BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "AED")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))

# -------------------------------------------------------------------
# We show time values in GMT+4
# -------------------------------------------------------------------

GST = timezone(timedelta(hours=4))

def to_gst(dt_str):
    if not dt_str:
        return None
    try:
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(GST).replace(tzinfo=None)  # store naive GST
    except:
        return None

# -------------------------------------------------------------------
# Rate Limiter
# -------------------------------------------------------------------

class TokenBucketRateLimiter:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                wait_time = (1.0 - self.tokens) / self.rate

            time.sleep(wait_time)


fees_rate_limiter = TokenBucketRateLimiter(rate=1.0, burst=2)

# -------------------------------------------------------------------
# Retry wrapper
# -------------------------------------------------------------------

def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    delay = initial_delay
    for attempt in range(max_retries):
        result = func(*args, **kwargs)

        if isinstance(result, dict) and "errors" in result:
            codes = [err.get("code") for err in result.get("errors", [])]
            if "QuotaExceeded" in codes or "RequestThrottled" in codes:
                if attempt < max_retries - 1:
                    print(f"Rate limit hit - Retry {attempt + 1}/{max_retries} after {delay:.1f}s")
                    time.sleep(delay)
                    delay *= 2
                    continue

        return result

    return result


def estimate_fees_for_item(sku, asin, price, counters):
    counters["sp_calls"] += 1

    def _fetch():
        fees_rate_limiter.acquire()
        return get_fees_estimate(sku, asin, price)

    return retry_api_call(_fetch)

# -------------------------------------------------------------------
# Async batch fee estimation
# -------------------------------------------------------------------

async def estimate_fees_batch_async(items, counters):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, estimate_fees_for_item, s, a, p, counters)
            for s, a, p in items
        ]
        return await asyncio.gather(*tasks)

# -------------------------------------------------------------------
# Main orders logic
# -------------------------------------------------------------------

async def get_orders_async(params):
    start_time = time.time()

    # ---------------------------------------------------------------
    # 1. Compute report window
    # ---------------------------------------------------------------
    last_updated_after = params.get("LastUpdatedAfter")
    created_after = params.get("CreatedAfter")
    created_before = params.get("CreatedBefore")

    if last_updated_after:
        end_dt = datetime.now(timezone.utc)
        try:
            start_dt = datetime.fromisoformat(last_updated_after.replace("Z", "+00:00"))
        except Exception:
            start_dt = end_dt - timedelta(hours=10)

    elif created_after and created_before:
        start_dt = datetime.fromisoformat(created_after.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(created_before.replace("Z", "+00:00"))

    else:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=10)

    report_type = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"

    # ---------------------------------------------------------------
    # 2. Request report
    # ---------------------------------------------------------------
    print(f"Requesting report for {start_dt.isoformat()} to {end_dt.isoformat()}")

    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": report_type,
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        print("Error: Failed to create report.", create_resp)
        return []

    report_id = create_resp["reportId"]

    # ---------------------------------------------------------------
    # 3. Wait for report to finish
    # ---------------------------------------------------------------
    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )

        if status_resp:
            status = status_resp.get("processingStatus")
            if status == "DONE":
                break
            if status in ("CANCELLED", "FATAL"):
                print(f"Error: Report processing failed: {status}")
                return []

        await asyncio.sleep(5)
    else:
        print("Timeout waiting for report to be DONE.")
        return []

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        print("Error: No reportDocumentId found.")
        return []

    # ---------------------------------------------------------------
    # 4. Download report
    # ---------------------------------------------------------------
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )

    if not doc_resp or "url" not in doc_resp:
        print("Error: Failed to get download URL", doc_resp)
        return []

    import requests
    raw = requests.get(doc_resp["url"]).content

    if doc_resp.get("compressionAlgorithm") == "GZIP":
        import gzip
        decoded = gzip.decompress(raw).decode("utf-8")
    else:
        decoded = raw.decode("utf-8")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    if not rows:
        print("No rows in report.")
        return []

    # ---------------------------------------------------------------
    # 5. Load product mapping + details
    # ---------------------------------------------------------------
    all_skus = list({r.get("sku") or r.get("SKU") for r in rows if (r.get("sku") or r.get("SKU"))})
    asin_list = list({r.get("asin") or r.get("ASIN") for r in rows if (r.get("asin") or r.get("ASIN"))})

    conn = connect_database()
    cursor = conn.cursor()
    try:
        product_mapping = get_product_mapping(cursor, all_skus) if all_skus else {}
        product_details = get_product_details_by_asin(cursor, asin_list) if asin_list else {}
    finally:
        cursor.close()
        conn.close()

    # ---------------------------------------------------------------
    # 6. Prepare fee estimation items
    # ---------------------------------------------------------------
    items_to_est = []
    report_items = []

    for r in rows:
        order_id = r.get("amazon-order-id") or r.get("AmazonOrderId")
        sku = r.get("sku") or r.get("SKU")
        asin = r.get("asin") or r.get("ASIN")

        qty_str = r.get("quantity") or r.get("Qty") or "1"
        try:
            qty = int(qty_str)
        except:
            qty = 1

        line_total_str = (
            r.get("item-price")
            or r.get("ItemPrice")
            or r.get("unit-price")
            or r.get("UnitPrice")
        )

        try:
            line_total = float(line_total_str) if line_total_str not in (None, "", "Not Available") else None
        except:
            line_total = None

        unit_price = None
        if line_total is not None and qty > 0:
            unit_price = line_total / qty

        if sku and asin and unit_price and unit_price > 0:
            items_to_est.append((sku, asin, round(unit_price, 2)))

        report_items.append({
            "raw_row": r,
            "order_id": order_id,
            "sku": sku,
            "asin": asin,
            "qty": qty,
            "unit_price": unit_price,
            "line_total": line_total,
            "row_index": len(report_items),
        })

    # ---------------------------------------------------------------
    # 7. Fee estimation
    # ---------------------------------------------------------------
    unique_items = list(set(items_to_est))
    print(f"Estimating fees for {len(unique_items)} unique items...")

    counters = {"sp_calls": 0}
    estimates = await estimate_fees_batch_async(unique_items, counters)
    fees_by_key = dict(zip(unique_items, estimates))

    # ---------------------------------------------------------------
    # 8. Build output rows
    # ---------------------------------------------------------------
    output = []

    for item in report_items:
        r = item["raw_row"]
        order_id = item["order_id"]
        sku = item["sku"]
        asin = item["asin"]
        qty = item["qty"]
        unit_price = item["unit_price"]

        mapping = product_mapping.get(sku, {})
        prod_details = product_details.get(asin, {})

        ssku = mapping.get("ssku") if mapping else sku
        brand = prod_details.get("brand")
        category = prod_details.get("category")
        title = (
            prod_details.get("item_name")
            or prod_details.get("title")
            or r.get("product-name")
            or r.get("ProductName")
            or r.get("Title")
        )

        line_total = item["line_total"]
        subtotal = line_total

        # -----------------------------------------------------------
        # Fee pipeline
        # -----------------------------------------------------------
        fee_incl = None
        fee_pct = None
        fba_fees_incl = None
        total_fee = None
        rvat = None
        vat = None
        cog = None
        profit = None

        if sku and asin and unit_price and unit_price > 0 and qty > 0:
            fees = fees_by_key.get((sku, asin, round(unit_price, 2)))
            f_net = fees.get("net") if isinstance(fees, dict) else None

            referral_per_unit = float(f_net.get("ReferralFees", 0.0)) if f_net else 0.0
            fba_per_unit = float(f_net.get("FBAFees", 0.0)) if f_net else 0.0

            ref_total = referral_per_unit * AMAZON_VAT_MULTIPLIER * qty
            fba_total = fba_per_unit * AMAZON_VAT_MULTIPLIER * qty
            total_fee_val = ref_total + fba_total

            fee_incl = -ref_total if ref_total else None
            fba_fees_incl = -fba_total if fba_total else None
            total_fee = -total_fee_val if total_fee_val else None

            if unit_price:
                try:
                    fee_pct = (referral_per_unit / unit_price) * 100
                except:
                    fee_pct = None

            subtotal_val = unit_price * qty
            vat_total = subtotal_val * GOVT_VAT_RATE if subtotal_val is not None else None
            rvat_total = (
                (referral_per_unit + fba_per_unit) * (AMAZON_VAT_MULTIPLIER - 1.0) * qty
                if AMAZON_VAT_MULTIPLIER > 1.0
                else 0.0
            )

            vat = -vat_total if vat_total else None
            rvat = rvat_total if rvat_total else None

            cost = parse_cost(prod_details.get("cost")) if prod_details else None
            cog_total = cost * qty if cost is not None else None
            cog = -cog_total if cog_total is not None else None

            # -------------------------------------------------------
            # Profit must be None if ANY dependency is missing
            # -------------------------------------------------------
            if (
                subtotal_val is not None
                and total_fee_val is not None
                and vat_total is not None
                and rvat_total is not None
                and cog_total is not None
            ):
                profit = subtotal_val - total_fee_val - vat_total + rvat_total - cog_total
            else:
                profit = None

        currency = r.get("currency") or BASE_CURRENCY_CODE

        output.append({
            "AmazonOrderId": order_id,
            "OrderDate": to_gst(r.get("purchase-date") or r.get("OrderDate")),
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
            "FeeIncl": fee_incl,
            "FeePct": fee_pct,
            "FBAFeesIncl": fba_fees_incl,
            "TotalFee": total_fee,
            "RVAT": rvat,
            "VAT": vat,
            "COG": cog,
            "Profit": profit,
            "Refund": None,
            "RefundDate": None,
            "ReturnDate": None,
            "ReturnDisposition": None,
            "ReturnReason": None,
            "LicensePlateNumber": None,
            "Reimbursed": None,
            "ReimbDate": None,
            "RemovalDate": None,
            "RemovalId": None,
            "RemovalTracking": None,
            "RemovalDelivery": None,
            "OrderStatus": r.get("order-status") or r.get("OrderStatus"),
            "LastUpdateDate": to_gst(r.get("last-updated-date") or r.get("LastUpdateDate")),
            "FirstSeenAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "LastSeenAt": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
        })

    print(f"SUMMARY\nOrderItems rows: {len(output)}\nTime: {(time.time() - start_time) / 60:.1f}m")
    return output


async def get_orders(params):
    return await get_orders_async(params)
