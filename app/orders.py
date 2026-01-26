#!/usr/bin/env python3
# (updated orders.py with robust VAT / RVAT handling)
import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Dict, List, Any, Optional
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

# ENV and VAT config
# Keep GOVT_VAT_RATE derived as 1 / GOVT_VAT_RATE_DIVISOR per your requirement (default 21)
try:
    GOVT_VAT_RATE_DIVISOR = float(os.getenv("GOVT_VAT_RATE_DIVISOR", "21"))
    GOVT_VAT_RATE = 1.0 / GOVT_VAT_RATE_DIVISOR
except Exception:
    GOVT_VAT_RATE_DIVISOR = 21.0
    GOVT_VAT_RATE = 1.0 / GOVT_VAT_RATE_DIVISOR

BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
FEE_CACHE_TTL_DAYS = int(os.getenv("FEE_CACHE_TTL_DAYS", "7"))
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


# small helpers for VAT/fee parsing
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None


def _vat_from_net(net_amount: Optional[float]) -> Optional[float]:
    if net_amount is None:
        return None
    return net_amount * GOVT_VAT_RATE


def _vat_from_gross(gross_amount: Optional[float]) -> Optional[float]:
    if gross_amount is None:
        return None
    v = GOVT_VAT_RATE
    return gross_amount * (v / (1.0 + v))


def _pick_first_nonnull(*vals):
    for v in vals:
        if v is not None:
            return v
    return None


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

            # Determine unit price:
            # Primary: if a SubTotal exists in the item payload treat that as the subtotal for the line
            # and compute unit_price = subtotal / qty. Otherwise fall back to ItemPrice/ListPrice or fallback_prices.
            subtotal_value = None

            # common keys we might encounter
            if isinstance(item.get("SubTotal"), dict):
                subtotal_value = item.get("SubTotal", {}).get("Amount")
            elif item.get("SubTotal") is not None:
                # sometimes SubTotal is a raw numeric/string
                subtotal_value = item.get("SubTotal")

            # some variations: ItemPrice could be subtotal in your test data -> use it only when SubTotal not provided
            if subtotal_value is None and isinstance(item.get("ItemPrice"), dict):
                # If ItemPrice.Amount is present but qty>1 and business says it's subtotal, use it divided by qty.
                subtotal_value = item.get("ItemPrice", {}).get("Amount")

            unit_price = 0.0
            subtotal = None
            if subtotal_value is not None:
                try:
                    subtotal = float(subtotal_value)
                    unit_price = subtotal / (qty if qty else 1)
                except Exception:
                    unit_price = fallback_prices.get(sku, 0.0)
            else:
                # fallback: try ListPrice or ItemPrice (per-unit)
                per_unit = None
                if isinstance(item.get("ListPrice"), dict):
                    per_unit = item.get("ListPrice", {}).get("Amount")
                elif isinstance(item.get("ItemPrice"), dict):
                    per_unit = item.get("ItemPrice", {}).get("Amount")
                try:
                    unit_price = float(per_unit) if per_unit is not None else fallback_prices.get(sku, 0.0)
                    subtotal = unit_price * qty if unit_price else None
                except Exception:
                    unit_price = fallback_prices.get(sku, 0.0)
                    subtotal = unit_price * qty if unit_price else None

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
                    "subtotal": subtotal,
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

        # Title should come from product/details (CurrentInventory), not from API
        title = d.get("item_name") or d.get("title") or d.get("name") or (meta["m"].get("title") if meta.get("m") else None) or "Not Available"

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
            # SOLD is per-unit price
            "SOLD": meta["unit_price"],
            "Brand": d.get("brand", "Not Available"),
            "Category": d.get("category", "Not Available"),
            "Title": title,
        }

        # Compute fees/VAT/RVAT robustly
        if f and isinstance(f, dict) and meta["unit_price"] > 0:
            # extract possible keys (try several common variants)
            ref_ex = _safe_float(_pick_first_nonnull(f.get("ReferralFees"), f.get("ReferralFee"), f.get("ReferralFee.Amount"), f.get("ReferralFeesAmount")))
            ref_inc = _safe_float(_pick_first_nonnull(f.get("ReferralFeesIncl"), f.get("ReferralFeesWithTax"), f.get("ReferralFeeInclusive"), f.get("ReferralFeesGross")))

            fba_ex = _safe_float(_pick_first_nonnull(f.get("FBAFees"), f.get("FBAFee"), f.get("FBAFeesAmount")))
            fba_inc = _safe_float(_pick_first_nonnull(f.get("FBAFeesIncl"), f.get("FBAFeesWithTax"), f.get("FBAFeeInclusive"), f.get("FBAFeesGross")))

            # displayed (signed) amounts: prefer inclusive (gross) if available, else exclusive (net)
            ref_display = None
            if ref_inc is not None:
                ref_display = -ref_inc * AMAZON_VAT_MULTIPLIER
            elif ref_ex is not None:
                ref_display = -ref_ex * AMAZON_VAT_MULTIPLIER

            fba_display = None
            if fba_inc is not None:
                fba_display = -fba_inc * AMAZON_VAT_MULTIPLIER
            elif fba_ex is not None:
                fba_display = -fba_ex * AMAZON_VAT_MULTIPLIER

            total_fee_display = None
            if ref_display is not None or fba_display is not None:
                total_fee_display = (ref_display or 0.0) + (fba_display or 0.0)

            # fee percent - prefer referral exclusive amount for percentage calculation
            fee_pct = None
            ref_base_for_pct = _pick_first_nonnull(ref_ex, (abs(ref_display) if ref_display is not None else None))
            if meta["unit_price"] and ref_base_for_pct:
                try:
                    fee_pct = float(ref_base_for_pct) / float(meta["unit_price"]) * 100.0
                except Exception:
                    fee_pct = None

            # RVAT: compute VAT portion attributable to referral+FBA fees
            parts = []
            if ref_inc is not None and ref_ex is not None:
                parts.append(ref_inc - ref_ex)
            elif ref_inc is not None:
                parts.append(_vat_from_gross(ref_inc))
            elif ref_ex is not None:
                parts.append(_vat_from_net(ref_ex))

            if fba_inc is not None and fba_ex is not None:
                parts.append(fba_inc - fba_ex)
            elif fba_inc is not None:
                parts.append(_vat_from_gross(fba_inc))
            elif fba_ex is not None:
                parts.append(_vat_from_net(fba_ex))

            if parts:
                rvat_value = sum([p for p in parts if p is not None])
                rvat = -rvat_value
            else:
                rvat = None

            # per-unit VAT on item price (always compute if unit_price present)
            vat = None
            if meta["unit_price"]:
                vat = -_vat_from_net(meta["unit_price"])
            else:
                vat = None

            t.update(
                {
                    "Est Fee": ref_display if ref_display is not None else "Not Available",
                    "Est FBAFees": fba_display if fba_display is not None else "Not Available",
                    "Est TotalAmazonFees": total_fee_display if total_fee_display is not None else "Not Available",
                    "Est R. VAT": rvat if rvat is not None else "Not Available",
                    "Est Fee%": fee_pct if fee_pct is not None else "Not Available",
                    "VAT": vat if vat is not None else "Not Available",
                }
            )

            c = parse_cost(d.get("cost"))
            t["COG"] = -c if c is not None else "Not Available"

            t["Est Net Profit"] = (
                meta["unit_price"] - (abs(ref_display or 0) + abs(fba_display or 0)) - (-(vat or 0)) + (rvat if rvat is not None else 0) - c
                if c is not None
                else "Not Available"
            )
        else:
            # No fees available — still compute VAT per unit if possible
            vat = -_vat_from_net(meta["unit_price"]) if meta["unit_price"] else None
            t.update(
                {
                    "Est Fee": "Not Available",
                    "Est FBAFees": "Not Available",
                    "Est TotalAmazonFees": "Not Available",
                    "Est R. VAT": "Not Available",
                    "Est Fee%": "Not Available",
                    "VAT": vat if vat is not None else "Not Available",
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