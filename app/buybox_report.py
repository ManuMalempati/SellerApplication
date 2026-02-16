import os
import time
import csv
import threading
import asyncio
from io import StringIO
from concurrent.futures import ThreadPoolExecutor
import requests

from .auth import spapi_request
from .database import (
    connect_database,
    get_all_product_mapping,
    get_product_details_by_asin,
    parse_cost,
)
from .estimates import get_fees_estimate

# ---------------------------------------------------------
# Config
# ---------------------------------------------------------

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))

_divisor_raw = os.getenv("GOVT_VAT_RATE_DIVISOR")
if _divisor_raw:
    try:
        _divisor_val = float(_divisor_raw)
        GOVT_VAT_RATE = 1 / _divisor_val if _divisor_val != 0 else 0.0
    except ValueError:
        GOVT_VAT_RATE = 0.0
else:
    GOVT_VAT_RATE = 0.0

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5.0

# ---------------------------------------------------------
# Rate Limiters
# ---------------------------------------------------------

class TokenBucketRateLimiter:
    def __init__(self, rate, burst):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last = now
                if self.tokens >= 1:
                    self.tokens -= 1
                    return
                wait = (1 - self.tokens) / self.rate
            time.sleep(wait)


# getPricing limits: 0.5 RPS, burst 1 → 1 request every 2 seconds
pricing_limiter = TokenBucketRateLimiter(rate=0.5, burst=1)

# Fees API: 1 RPS, burst 2
fees_limiter = TokenBucketRateLimiter(rate=1.0, burst=2)

pricing_progress = {"done": 0, "total": 0}
fees_progress = {"done": 0, "total": 0}
progress_lock = threading.Lock()

# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def throttle():
    time.sleep(0.5)


def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    delay = initial_delay
    resp = None
    for attempt in range(max_retries):
        resp = func(*args, **kwargs)
        if isinstance(resp, dict) and "errors" in resp:
            codes = [e.get("code") for e in resp["errors"]]
            if "QuotaExceeded" in codes or "RequestThrottled" in codes:
                if attempt < max_retries - 1:
                    print(f"[RETRY] Throttled {codes}, retry {attempt+1}/{max_retries} in {delay}s")
                    time.sleep(delay)
                    delay *= 2
                    continue
        # Either success or non-throttling error → return as-is
        return resp
    return resp


def request_report(report_type, params=None):
    throttle()
    print(f"📦 Requesting report: {report_type}")
    body = {"reportType": report_type, "marketplaceIds": [MARKETPLACE_ID]}
    if params:
        body.update(params)

    resp = spapi_request("POST", "/reports/2021-06-30/reports", body=body) or {}
    print("📦 Report request response:", resp)

    report_id = resp.get("reportId")
    if not report_id:
        print("❌ No reportId in response, aborting.")
        return None
    return report_id


def wait_for_report(report_id, timeout=300):
    if not report_id:
        raise ValueError("wait_for_report called with empty report_id")

    print(f"⏳ Waiting for report {report_id}...")
    start = time.time()

    while True:
        throttle()
        resp = spapi_request("GET", f"/reports/2021-06-30/reports/{report_id}") or {}
        status = resp.get("processingStatus") or "UNKNOWN"
        print(f"   → Status: {status}")

        if status == "DONE":
            print("📄 Report is DONE")
            doc_id = resp.get("reportDocumentId")
            if not doc_id:
                raise ValueError("Report DONE but no reportDocumentId in response")
            return doc_id

        if time.time() - start > timeout:
            raise TimeoutError("Report timed out")

        time.sleep(2)


def download_report(document_id):
    if not document_id:
        raise ValueError("download_report called with empty document_id")

    print(f"⬇️ Downloading report document {document_id}...")
    throttle()

    doc = spapi_request("GET", f"/reports/2021-06-30/documents/{document_id}") or {}
    url = doc.get("url")
    if not url:
        raise ValueError("No URL in report document response")

    raw = requests.get(url).content
    if doc.get("compressionAlgorithm") == "GZIP":
        import gzip
        raw = gzip.decompress(raw)

    print("📄 Report downloaded.")
    return raw.decode("utf-8")

# ---------------------------------------------------------
# Pricing API (getPricing) — BATCHED (20 SKUs per call)
# ---------------------------------------------------------

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def _pricing_call_batch(sku_list):
    # Respect Amazon quota: 0.5 RPS, burst 1
    pricing_limiter.acquire()

    params = {
        "MarketplaceId": MARKETPLACE_ID,
        "ItemType": "Sku",
        # IMPORTANT: must be comma-joined string, not list
        "Skus": ",".join(sku_list),
        "OfferType": "B2C",
        "ItemCondition": "New",
    }

    return spapi_request("GET", "/products/pricing/v0/price", params=params)


def fetch_pricing_batch(sku_list):
    resp = retry_api_call(_pricing_call_batch, sku_list)

    # If still errors after retries, log and return empty
    if isinstance(resp, dict) and "errors" in resp:
        print(f"[pricing] ERROR for batch {sku_list}: {resp['errors']}")
        return {sku: None for sku in sku_list}

    if not isinstance(resp, dict):
        print(f"[pricing] Non-dict response for batch {sku_list}: {resp}")
        return {sku: None for sku in sku_list}

    out = {}
    payload = resp.get("payload") or []

    for entry in payload:
        sku = entry.get("SellerSKU")
        offers = entry.get("Product", {}).get("Offers") or []

        if not sku:
            continue

        if not offers:
            print(f"[pricing] No offers for {sku}: {entry}")
            out[sku] = None
            continue

        amount = (
            offers[0]
            .get("BuyingPrice", {})
            .get("ListingPrice", {})
            .get("Amount")
        )

        out[sku] = float(amount) if amount is not None else None

    # Fill missing SKUs (Amazon sometimes omits them)
    for sku in sku_list:
        if sku not in out:
            print(f"[pricing] Missing SKU in payload: {sku}")
            out[sku] = None

    return out


async def run_pricing_batch(skus):
    pricing_progress["done"] = 0
    pricing_progress["total"] = len(skus)

    print(f"💰 Fetching prices for {len(skus)} SKUs...")

    loop = asyncio.get_event_loop()
    results = {}

    # Only 1 worker to respect rate limits
    with ThreadPoolExecutor(max_workers=1) as ex:
        for batch in chunk(skus, 20):
            batch_dict = await loop.run_in_executor(ex, fetch_pricing_batch, batch)
            results.update(batch_dict)

            with progress_lock:
                pricing_progress["done"] += len(batch)
                done = pricing_progress["done"]
                total = pricing_progress["total"]
                print(f"💰 Pricing progress: {done}/{total} ({100 * done // total}%)")

            # getPricing = 1 request every 2 seconds
            time.sleep(2)

    print("💰 Pricing batch complete.")
    return results

# ---------------------------------------------------------
# Fees API
# ---------------------------------------------------------

def _fees_call(sku, asin, price):
    fees_limiter.acquire()
    return get_fees_estimate(sku, asin, price)


def estimate_fees(sku, asin, price):
    return retry_api_call(_fees_call, sku, asin, price)


def estimate_fees_worker(sku, asin, price):
    result = (sku, asin, price), estimate_fees(sku, asin, price)
    with progress_lock:
        fees_progress["done"] += 1
        done = fees_progress["done"]
        total = fees_progress["total"]
        if total and (done % 20 == 0 or done == total):
            print(f"📊 Fees progress: {done}/{total} ({100 * done // total}%)")
    return result


async def run_fees_batch(items):
    fees_progress["done"] = 0
    fees_progress["total"] = len(items)

    print(f"📊 Estimating fees for {len(items)} items...")

    if not items:
        print("📊 No items to estimate fees for.")
        return {}

    loop = asyncio.get_event_loop()
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        tasks = [
            loop.run_in_executor(ex, estimate_fees_worker, sku, asin, price)
            for (sku, asin, price) in items
        ]
        batch_results = await asyncio.gather(*tasks)

    for d in batch_results:
        key, val = d
        results[key] = val

    print("📊 Fee batch complete.")
    return results

# ---------------------------------------------------------
# MAIN BUYBOX FUNCTION
# ---------------------------------------------------------

async def buyboxes():
    start_time = time.time()

    print("🔌 Loading product mapping...")
    conn = connect_database()
    cursor = conn.cursor()
    product_mappings = get_all_product_mapping(cursor) or {}
    cursor.close()
    conn.close()

    report_id = request_report("GET_AFN_INVENTORY_DATA")
    print(f"📦 Report requested: {report_id}")

    if not report_id:
        raise RuntimeError("Failed to request report: no report_id returned")

    document_id = wait_for_report(report_id)
    print(f"📄 Report ready: {document_id}")

    raw_text = download_report(document_id)

    rows = []
    reader = csv.DictReader(StringIO(raw_text), delimiter="\t")
    for line in reader:
        sku = line.get("seller-sku")
        asin = line.get("asin")
        qty = line.get("Quantity Available")
        fnsku = line.get("fulfillment-channel-sku")

        if not sku:
            continue

        # FILTER: Only include SKUs that exist in ProductMapping
        if sku not in product_mappings:
            continue

        qty_int = int(qty) if qty and qty.isdigit() else 0

        # Client has told to ignore this filter for now.
        # if qty_int <= 0:
        #     continue

        ssku = (product_mappings.get(sku) or {}).get("ssku")
        rows.append(
            {
                "SKU": sku,
                "ASIN": asin,
                "SSKU": ssku,
                "FNSKU": fnsku,
                "FBA-Stock": qty_int,
            }
        )

    print(f"📦 Parsed {len(rows)} items (filtered to ProductMapping)")

    asins = list({r["ASIN"] for r in rows if r["ASIN"]})
    print(f"🔍 Loading product details for {len(asins)} ASINs...")

    conn = connect_database()
    cursor = conn.cursor()
    product_details = get_product_details_by_asin(cursor, asins) or {}
    cursor.close()
    conn.close()

    for r in rows:
        d = product_details.get(r["ASIN"]) or {}
        r["Title"] = d.get("item_name")
        r["COG"] = d.get("cost")
        r["Brand"] = d.get("brand")
        r["Category"] = d.get("category")

    # -----------------------------------------------------
    # Pricing
    # -----------------------------------------------------

    skus = [r["SKU"] for r in rows]
    pricing = await run_pricing_batch(skus)

    fee_items = []
    for r in rows:
        price = pricing.get(r["SKU"])
        r["Sale-Price"] = price
        if price and r["ASIN"]:
            fee_items.append((r["SKU"], r["ASIN"], price))

    # -----------------------------------------------------
    # Fees
    # -----------------------------------------------------

    fees = await run_fees_batch(fee_items)

    for r in rows:
        sku = r["SKU"]
        asin = r["ASIN"]
        price = r["Sale-Price"]

        if price and asin:
            f = fees.get((sku, asin, price)) or {}
            net = f.get("net") or {}

            ref = float(net.get("ReferralFees", 0) or 0)
            fba = float(net.get("FBAFees", 0) or 0)
            vat = price * GOVT_VAT_RATE
            cog = parse_cost(r["COG"]) or 0

            r["Est-Fee"] = -ref if ref else None
            r["Est-FBA Fee"] = -fba if fba else None
            r["Est.VAT"] = -vat
            r["Est-Net"] = price - ref - fba - vat - cog
        else:
            r["Est-Fee"] = None
            r["Est-FBA Fee"] = None
            r["Est.VAT"] = None
            r["Est-Net"] = None

    elapsed = time.time() - start_time

    print("=" * 60)
    print("BUYBOX REPORT - SUMMARY")
    print("=" * 60)
    print(f"Total items with stock: {len(rows)}")
    print(f"Items with price: {len([r for r in rows if r['Sale-Price']])}")
    print(f"Items with fees: {len([r for r in rows if r['Est-Fee'] is not None])}")
    print(f"Total time: {elapsed:.1f}s")
    print("=" * 60)

    # return rows
    return {
        "total_items": len(rows),
        "items_with_price": len([r for r in rows if r["Sale-Price"]]),
        "items_with_fees": len([r for r in rows if r["Est-Fee"] is not None]),
        "execution_time_seconds": round(elapsed, 2),
        "items": rows,
    }
