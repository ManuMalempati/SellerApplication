#!/usr/bin/env python3
import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, List, Any
import threading

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

GOVT_VAT_RATE = 1 / float(os.getenv("GOVT_VAT_RATE_DIVISOR", "1")) if os.getenv("GOVT_VAT_RATE_DIVISOR") else 0.0
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


# -------------------------------------------------------------------
# Rate Limiters
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


orders_rate_limiter = TokenBucketRateLimiter(rate=0.0167, burst=20)
order_items_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=30)
pricing_rate_limiter = TokenBucketRateLimiter(rate=0.4, burst=1)
fees_rate_limiter = TokenBucketRateLimiter(rate=1.0, burst=2)


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    delay = initial_delay
    for attempt in range(max_retries):
        result = func(*args, **kwargs)

        if isinstance(result, dict) and "errors" in result:
            codes = [err.get("code") for err in result.get("errors", [])]
            if "QuotaExceeded" in codes or "RequestThrottled" in codes:
                if attempt < max_retries - 1:
                    print("Rate limit hit - Retry {}/{} after {:.1f}s".format(attempt + 1, max_retries, delay))
                    time.sleep(delay)
                    delay *= 2
                    continue

        return result

    return result

def retrieve_orders_list(method, path, params):
    orders = []
    orders_rate_limiter.acquire()

    resp = spapi_request(method=method, path=path, params=params)
    if "errors" in resp:
        return orders

    payload = resp.get("payload") or {}
    orders.extend(payload.get("Orders", []))
    next_token = payload.get("NextToken")

    while next_token:
        orders_rate_limiter.acquire()
        resp = spapi_request(method=method, path=path, params={"NextToken": next_token})

        if "errors" in resp:
            break

        payload = resp.get("payload") or {}
        orders.extend(payload.get("Orders", []))
        next_token = payload.get("NextToken")

    return orders


def get_single_order_items(order_id):
    def _fetch():
        order_items_rate_limiter.acquire()
        return spapi_request("GET", f"/orders/v0/orders/{order_id}/orderItems")

    resp = retry_api_call(_fetch)
    return resp.get("payload", {}).get("OrderItems", []) if "payload" in resp else []


async def get_order_items_batch_async(order_ids: List[str]):
    loop = asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [loop.run_in_executor(executor, get_single_order_items, oid) for oid in order_ids]
        results = await asyncio.gather(*tasks)

    return dict(zip(order_ids, results))


# -------------------------------------------------------------------
# Fee Estimation (always enabled)
# -------------------------------------------------------------------

def estimate_fees_for_item(sku, asin, price, counters):
    counters["sp_calls"] += 1

    def _fetch():
        fees_rate_limiter.acquire()
        return get_fees_estimate(sku, asin, price)

    return retry_api_call(_fetch)


async def estimate_fees_batch_async(items, counters):
    loop = asyncio.get_event_loop()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, estimate_fees_for_item, s, a, p, counters)
            for s, a, p in items
        ]
        return await asyncio.gather(*tasks)


# -------------------------------------------------------------------
# Main Orders Logic
# -------------------------------------------------------------------

async def get_orders_async(params):
    start_time = time.time()

    print("Fetching orders...")
    orders = retrieve_orders_list("GET", "/orders/v0/orders", params)
    if not orders:
        print("SUMMARY\nOrders: 0")
        return []

    print("Retrieved {} orders".format(len(orders)))
    print("Fetching order items...")
    print("Estimated time: {:.1f} minutes".format(len(orders) / 0.5 / 60))

    order_ids = [o["AmazonOrderId"] for o in orders]
    order_items_map = await get_order_items_batch_async(order_ids)

    # -------------------------------------------------------------------
    # SHORT-LIVED DB CONNECTION (mapping + details only)
    # -------------------------------------------------------------------
    conn = connect_database()
    cursor = conn.cursor()
    try:
        all_skus = {
            item["SellerSKU"]
            for items in order_items_map.values()
            for item in items
            if "SellerSKU" in item
        }

        mapping = get_product_mapping(cursor, list(all_skus))
        asin_list = [m["asin"] for m in mapping.values() if m.get("asin")]
        details = get_product_details_by_asin(cursor, asin_list)

    finally:
        cursor.close()
        conn.close()
    # -------------------------------------------------------------------

    # -------------------------------------------------------------------
    # Build metadata
    # -------------------------------------------------------------------
    metadata = []
    items_to_est = []

    for order in orders:
        oid = order["AmazonOrderId"]
        order_status = order.get("OrderStatus")
        order_date = order.get("PurchaseDate") or order.get("OrderDate") or ""
        last_update_date = order.get("LastUpdateDate") or order.get("LastUpdatedDate") or ""

        for item in order_items_map.get(oid, []):
            sku = item.get("SellerSKU")
            m = mapping.get(sku)
            if not sku or not m or not m.get("asin"):
                continue

            asin = m["asin"]

            # Quantity
            qty_raw = item.get("QuantityOrdered", 1)
            try:
                qty = int(qty_raw) if qty_raw is not None else 1
            except Exception:
                qty = 1

            # ----- UNIT PRICE FIX -----
            unit_price = None

            # Prefer SubTotal if present
            subtotal_field = None
            if isinstance(item.get("SubTotal"), dict):
                subtotal_field = item["SubTotal"].get("Amount")
            elif item.get("SubTotal") is not None:
                subtotal_field = item.get("SubTotal")

            if subtotal_field is not None:
                try:
                    unit_price = float(subtotal_field) / max(qty, 1)
                except Exception:
                    unit_price = None
            else:
                # Fallback: ItemPrice.Amount is TOTAL for the line
                item_price_total = None
                if isinstance(item.get("ItemPrice"), dict):
                    item_price_total = item["ItemPrice"].get("Amount")

                if item_price_total is not None:
                    try:
                        unit_price = float(item_price_total) / max(qty, 1)
                    except Exception:
                        unit_price = None

            # Only estimate fees when price is valid
            if unit_price is not None and unit_price > 0:
                items_to_est.append((sku, asin, round(unit_price, 2)))


            metadata.append(
                {
                    "oid": oid,
                    "order_item_id": item.get("OrderItemId", ""),
                    "order_status": order_status,
                    "order_date": order_date,
                    "last_update_date": last_update_date,
                    "qty": qty,
                    "sku": sku,
                    "asin": asin,
                    "m": m,
                    "unit_price": unit_price,
                    "item": item,
                }
            )

    # -------------------------------------------------------------------
    # Fee estimation
    # -------------------------------------------------------------------
    unique_items = list(set(items_to_est))
    print("Estimating fees for {} unique items".format(len(unique_items)))

    counters = {"sp_calls": 0}
    estimates = await estimate_fees_batch_async(unique_items, counters)
    fees_by_key = dict(zip(unique_items, estimates))

    # -------------------------------------------------------------------
    # Build final rows (totals)
    # -------------------------------------------------------------------
    out = []

    for meta in metadata:
        sku = meta["sku"]
        asin = meta["asin"]
        unit_price = meta["unit_price"]
        qty = meta["qty"]
        subtotal = unit_price * qty if unit_price is not None else 0.0

        # Fix for None unit_price: never round None
        if unit_price is not None:
            price_key = round(unit_price, 2)
        else:
            price_key = None

        f = fees_by_key.get((sku, asin, price_key))

        # Fix: Safe fee extraction
        f_net = f.get("net") if isinstance(f, dict) else None
        referral_per_unit = float(f_net.get("ReferralFees", 0.0)) if f_net and f_net.get("ReferralFees") is not None else 0.0
        fba_per_unit = float(f_net.get("FBAFees", 0.0)) if f_net and f_net.get("FBAFees") is not None else 0.0

        # Fix: always numbers for total calculations
        referral_per_unit = referral_per_unit or 0.0
        fba_per_unit = fba_per_unit or 0.0

        # Totals (fix math with None)
        ref_total = referral_per_unit * AMAZON_VAT_MULTIPLIER * qty if unit_price is not None else None
        fba_total = fba_per_unit * AMAZON_VAT_MULTIPLIER * qty if unit_price is not None else None
        total_fee = (ref_total or 0.0) + (fba_total or 0.0) if unit_price is not None else None

        vat_total = subtotal * GOVT_VAT_RATE if subtotal is not None else None
        rvat_total = ((referral_per_unit + fba_per_unit) * (AMAZON_VAT_MULTIPLIER - 1.0)) * qty if unit_price is not None else None

        cost_per_unit = parse_cost(details.get(asin, {}).get("cost")) if asin else None
        cog_total = cost_per_unit * qty if cost_per_unit is not None else None

        # Fix: Safe fee percent calculation (no zero or None)
        if unit_price not in (None, 0):
            fee_pct = (referral_per_unit / unit_price) * 100
        else:
            fee_pct = None

        # Fix: Safe profit calculation (all prereqs must be valid, never None, never zero)
        if (
            unit_price not in (None, 0)
            and cost_per_unit is not None
            and total_fee is not None
            and vat_total is not None
            and rvat_total is not None
            and cog_total is not None
        ):
            profit_value = subtotal - total_fee - vat_total + rvat_total - cog_total
        else:
            profit_value = None

        t = {
            "OrderItemKey": "{}:{}:{}:{}".format(
                meta["oid"],
                meta["order_item_id"],
                sku,
                asin,
            ),
            "AmazonOrderId": meta["oid"],
            "OrderItemId": meta["order_item_id"],
            "OrderDate": meta["order_date"],
            "SKU": sku,
            "ASIN": asin,
            "SSKU": meta["m"].get("ssku", "Not Available"),
            "Brand": details.get(asin, {}).get("brand", "Not Available"),
            "Category": details.get(asin, {}).get("category", "Not Available"),
            "Title": details.get(asin, {}).get("item_name", "Not Available"),
            "Qty": qty,
            "UnitPrice": unit_price,
            "Subtotal": subtotal,
            "Currency": meta["item"].get("ItemPrice", {}).get("CurrencyCode", BASE_CURRENCY_CODE),
            "OrderStatus": meta["order_status"],
            "LastUpdateDate": meta["last_update_date"],
        }

        t.update(
            {
                "FeeIncl": -ref_total if ref_total is not None else None,
                "FBAFeesIncl": -fba_total if fba_total is not None else None,
                "TotalFee": -total_fee if total_fee is not None else None,
                "VAT": -vat_total if vat_total is not None else None,
                "RVAT": rvat_total if rvat_total is not None else None,
                "FeePct": fee_pct,
                "COG": -cog_total if cog_total is not None else None,
                "Profit": profit_value,
            }
        )

        out.append(t)

    print(
        "SUMMARY\nOrders: {}\nOrderItems rows: {}\nTime: {:.1f}m".format(
            len(orders), len(out), (time.time() - start_time) / 60
        )
    )

    return out


async def get_orders(params):
    return await get_orders_async(params)