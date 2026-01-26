import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any
import threading
import pyodbc

from .auth import spapi_request
from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    get_fee_estimate_from_product_mapping,
    upsert_fee_estimate_to_product_mapping,
    connect_database,
)
from .estimates import get_fees_estimate

# ENV
GOVT_VAT_RATE = 1 / float(os.getenv("GOVT_VAT_RATE_DIVISOR", "1")) if os.getenv("GOVT_VAT_RATE_DIVISOR") else 0.0
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
FEE_CACHE_TTL_DAYS = int(os.getenv("FEE_CACHE_TTL_DAYS", "7"))
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


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


# Usage-plan-based limiters
orders_rate_limiter = TokenBucketRateLimiter(rate=0.0167, burst=20)
order_items_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=30)
pricing_rate_limiter = TokenBucketRateLimiter(rate=0.4, burst=1)
fees_rate_limiter = TokenBucketRateLimiter(rate=1.0, burst=2)


def verify_cursor(cursor):
    """Ensure the database cursor is still alive after long API waits"""
    if not cursor:
        return None
    try:
        cursor.execute("SELECT 1")
        return cursor
    except (pyodbc.OperationalError, pyodbc.Error):
        print("⚠️ Database connection lost during API wait. Reconnecting...")
        new_conn = connect_database()
        return new_conn.cursor()


def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    delay = initial_delay
    for attempt in range(max_retries):
        result = func(*args, **kwargs)
        if isinstance(result, dict) and "errors" in result:
            err_codes = [err.get("code") for err in result.get("errors", [])]
            if "QuotaExceeded" in err_codes or "RequestThrottled" in err_codes:
                if attempt < max_retries - 1:
                    print(f"⏳ Rate limit hit - Retry {attempt + 1}/{max_retries} after {delay:.1f}s")
                    time.sleep(delay)
                    delay *= 2
                    continue
        return result
    return result


def get_listing_prices_batch(sku_list: List[str]) -> Dict[str, float]:
    unique_skus = list(set(sku_list))
    fallback_map: Dict[str, float] = {}

    for i in range(0, len(unique_skus), 20):
        chunk = unique_skus[i : i + 20]

        def _fetch():
            pricing_rate_limiter.acquire()
            params = {
                "MarketplaceId": MARKETPLACE_ID,
                "Skus": ",".join(chunk),
                "ItemType": "Sku",
                "ItemCondition": "New",
            }
            return spapi_request("GET", "/products/pricing/v0/price", params=params)

        print(f"🔍 [pricing] Fetching fallback prices for batch of {len(chunk)} SKUs...")
        resp = retry_api_call(_fetch)

        if "payload" in resp:
            for item in resp["payload"]:
                sku = item.get("SellerSKU")
                offers = item.get("Product", {}).get("Offers", [])
                if offers:
                    p = offers[0].get("BuyingPrice", {}).get("ListingPrice", {}).get("Amount")
                    if p is not None:
                        fallback_map[sku] = float(p)
        elif "errors" in resp:
            print(f"⚠️ [pricing] Batch error: {resp.get('errors')}")

    return fallback_map


def retrieve_orders_list(method, path, params):
    all_orders = []
    orders_rate_limiter.acquire()
    resp = spapi_request(method=method, path=path, params=params)
    if "errors" in resp:
        return all_orders

    payload = resp.get("payload") or {}
    all_orders.extend(payload.get("Orders", []))
    next_token = payload.get("NextToken")

    while next_token:
        orders_rate_limiter.acquire()
        resp = spapi_request(method=method, path=path, params={"NextToken": next_token})
        if "errors" in resp:
            break
        payload = resp.get("payload") or {}
        all_orders.extend(payload.get("Orders", []))
        next_token = payload.get("NextToken")

    return all_orders


def get_single_order_items(order_id):
    def _fetch():
        order_items_rate_limiter.acquire()
        return spapi_request("GET", f"/orders/v0/orders/{order_id}/orderItems")

    resp = retry_api_call(_fetch)
    return resp.get("payload", {}).get("OrderItems", []) if "payload" in resp else []


async def get_order_items_batch_async(order_ids: List[str]) -> Dict[str, List[dict]]:
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [loop.run_in_executor(executor, get_single_order_items, oid) for oid in order_ids]
        results = await asyncio.gather(*tasks)
    return {oid: items for oid, items in zip(order_ids, results)}


def estimate_fees_for_item(sku, asin, price, cache, counters):
    cache_key = (sku, asin, price)
    if cache_key in cache:
        counters["mem_hits"] += 1
        return cache[cache_key]
    counters["mem_misses"] += 1

    conn = connect_database()
    cursor = conn.cursor()
    try:
        db_entry = get_fee_estimate_from_product_mapping(cursor, sku)
        if db_entry and db_entry.get("last_price") == price and db_entry.get("updated_at"):
            updated_at = db_entry["updated_at"]

            # Convert SQL naive datetime → UTC-aware
            if updated_at.tzinfo is None:
                updated_at = updated_at.replace(tzinfo=timezone.utc)

            if datetime.now(timezone.utc) - updated_at <= timedelta(days=FEE_CACHE_TTL_DAYS):
                counters["db_hits"] += 1
                cache[cache_key] = db_entry.get("fees")
                return db_entry.get("fees")


        def _fetch():
            fees_rate_limiter.acquire()
            return get_fees_estimate(sku, asin, price)

        counters["sp_calls"] += 1
        fees = retry_api_call(_fetch)
        if fees and isinstance(fees, dict) and "errors" not in fees:
            upsert_fee_estimate_to_product_mapping(cursor, sku, asin, price, fees)
            conn.commit()
            cache[cache_key] = fees
        return fees
    finally:
        cursor.close()
        conn.close()


async def estimate_fees_batch_async(items, cache, counters):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, estimate_fees_for_item, s, a, p, cache, counters)
            for s, a, p in items
        ]
        return await asyncio.gather(*tasks)


async def get_orders_async(params, db_cursor=None):
    """
    Returns ONE row per Amazon Order Item (does NOT explode by quantity).
    Includes QuantityOrdered as a field.
    """
    start_time = time.time()

    print("🔄 Fetching orders...")
    orders = retrieve_orders_list("GET", "/orders/v0/orders", params)
    if not orders:
        print("=" * 60 + "\n📊 SUMMARY\nOrders: 0\n" + "=" * 60)
        return []

    print(f"✅ Retrieved {len(orders)} orders")
    print("🔄 Fetching order items...")
    print(f"⏱️  This will take approximately {len(orders) / 0.5 / 60:.1f} minutes due to API rate limits")

    order_ids = [o["AmazonOrderId"] for o in orders]
    order_items_map = await get_order_items_batch_async(order_ids)

    # RE-VERIFY CURSOR (In case the long wait killed the connection)
    db_cursor = verify_cursor(db_cursor)

    all_skus = list(
        {
            item["SellerSKU"]
            for items in order_items_map.values()
            for item in items
            if "SellerSKU" in item
        }
    )
    mapping = get_product_mapping(db_cursor, all_skus) if db_cursor else {}
    details = (
        get_product_details_by_asin(db_cursor, list({m["asin"] for m in mapping.values() if "asin" in m}))
        if db_cursor
        else {}
    )

    # Fetch pricing only for SKUs where BOTH ItemPrice and ListPrice are missing
    missing_price_skus = []
    for items in order_items_map.values():
        for item in items:
            if not item.get("ItemPrice") and not item.get("ListPrice") and item.get("SellerSKU"):
                missing_price_skus.append(item["SellerSKU"])

    fallback_prices = get_listing_prices_batch(missing_price_skus) if missing_price_skus else {}

    # Build metadata (1 row per order item)
    items_to_est, metadata = [], []
    for order in orders:
        oid = order["AmazonOrderId"]
        order_status = order.get("OrderStatus")

        purchase_date = order.get("PurchaseDate") or ""
        last_update_date = order.get("LastUpdateDate") or order.get("LastUpdatedDate") or ""

        items = order_items_map.get(oid, [])
        for item in items:
            sku = item.get("SellerSKU")
            m = mapping.get(sku)
            if not sku or not m or not m.get("asin") or m["asin"] == "Not Available":
                continue

            # quantity (do NOT explode)
            qty = item.get("QuantityOrdered", 1)
            try:
                qty = int(qty) if qty is not None else 1
            except Exception:
                qty = 1

            p_raw = item.get("ItemPrice", {}).get("Amount") or item.get("ListPrice", {}).get("Amount")
            unit_price = float(p_raw) if p_raw else fallback_prices.get(sku, 0.0)

            if unit_price > 0:
                items_to_est.append((sku, m["asin"], unit_price))

            metadata.append(
                {
                    "oid": oid,
                    "order_item_id": item.get("OrderItemId", ""),
                    "status": order_status,
                    "purchase_date": purchase_date,
                    "last_update_date": last_update_date,
                    "item": item,
                    "quantity": qty,
                    "sku": sku,
                    "asin": m["asin"],
                    "m": m,
                    "unit_price": unit_price,
                }
            )

    # Fee estimates are per-unit price
    unique_items = list(set(items_to_est))
    print(f"🔄 Estimating fees for {len(unique_items)} unique items...")
    cache = {}
    counters = {"mem_hits": 0, "mem_misses": 0, "db_hits": 0, "db_misses": 0, "sp_calls": 0}
    estimates = await estimate_fees_batch_async(unique_items, cache, counters)
    fees_by_key = {unique_items[i]: estimates[i] for i in range(len(unique_items))}

    order_items_out = []
    for meta in metadata:
        f = fees_by_key.get((meta["sku"], meta["asin"], meta["unit_price"]))
        d = details.get(meta["asin"], {})

        # NOTE: keep SOLD as unit price (matches your current semantics).
        # If you want SOLD_TOTAL, compute unit_price * quantity in dashboard/DB.
        t = {
            "AmazonOrderId": meta["oid"],
            "OrderItemId": meta["order_item_id"],
            "OrderStatus": meta["status"],
            "PurchaseDate": meta["purchase_date"],
            "LastUpdateDate": meta["last_update_date"],
            "Quantity": meta["quantity"],
            "SKU": meta["sku"],
            "ASIN": meta["asin"],
            "SSKU": meta["m"].get("ssku", "Not Available"),
            "Currency": meta["item"].get("ItemPrice", {}).get("CurrencyCode", BASE_CURRENCY_CODE),
            "SOLD": meta["unit_price"],
            "Brand": d.get("brand", "Not Available"),
            "Category": d.get("category", "Not Available"),
        }

        if f and isinstance(f, dict) and meta["unit_price"] > 0:
            ref_w = f.get("ReferralFees", 0) * AMAZON_VAT_MULTIPLIER
            fba_w = f.get("FBAFees", 0) * AMAZON_VAT_MULTIPLIER

            t.update(
                {
                    "Est Fee": -ref_w,
                    "Est FBAFees": -fba_w,
                    "Est TotalAmazonFees": -(ref_w + fba_w),
                    "Est R. VAT": (ref_w + fba_w) - (f.get("ReferralFees", 0) + f.get("FBAFees", 0)),
                    "Est Fee%": (f.get("ReferralFees", 0) / meta["unit_price"]) * 100,
                }
            )

            # VAT is on item price (per unit) based on your GOVT_VAT_RATE
            v = meta["unit_price"] * GOVT_VAT_RATE
            t["VAT"] = -v

            c = parse_cost(d.get("cost"))
            t["COG"] = -c if c is not None else "Not Available"

            t["Est Net Profit"] = (
                meta["unit_price"] - (ref_w + fba_w) - v + t["Est R. VAT"] - c
                if c is not None
                else "Not Available"
            )
        else:
            t.update(
                {
                    "Est Fee": "Not Available",
                    "Est FBAFees": "Not Available",
                    "Est TotalAmazonFees": "Not Available",
                    "Est R. VAT": "Not Available",
                    "Est Fee%": "Not Available",
                    "VAT": -(meta["unit_price"] * GOVT_VAT_RATE),
                    "COG": -parse_cost(d.get("cost")) if d.get("cost") else "Not Available",
                    "Est Net Profit": "Not Available",
                }
            )

        order_items_out.append(t)

    print(
        "=" * 60
        + f"\n📊 SUMMARY\nOrders: {len(orders)}\nOrderItems rows: {len(order_items_out)}\nTime: {(time.time() - start_time) / 60:.1f}m\n"
        + "=" * 60
    )
    return order_items_out


async def get_orders(params, db_cursor=None):
    return await get_orders_async(params, db_cursor)