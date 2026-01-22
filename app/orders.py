import os
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta
from typing import Dict, Tuple, List, Any
import threading

from . auth import spapi_request
from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    get_fee_estimate_from_cache,
    upsert_fee_estimate_cache,
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

# Retry settings
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


# ---------------------------------------------------------
# Token Bucket Rate Limiter
# ---------------------------------------------------------
class TokenBucketRateLimiter: 
    """Thread-safe token bucket rate limiter"""
    
    def __init__(self, rate: float, burst:  int):
        """
        Args:
            rate:  Requests per second
            burst: Maximum burst capacity (tokens)
        """
        self. rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()
    
    def acquire(self):
        """Wait until a token is available, then consume it"""
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                
                # Add tokens based on elapsed time
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now
                
                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return
                
                # Calculate wait time
                wait_time = (1.0 - self.tokens) / self.rate
            
            # Sleep outside the lock
            time.sleep(wait_time)


# Initialize rate limiters for different endpoints
orders_rate_limiter = TokenBucketRateLimiter(rate=0.0167, burst=20)  # Get Orders
order_items_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=30)  # Get Order Items
financials_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=30)  # Get Financial Events


# ---------------------------------------------------------
# Retry with exponential backoff
# ---------------------------------------------------------
def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    """Retry function with exponential backoff"""
    delay = initial_delay
    
    for attempt in range(max_retries):
        result = func(*args, **kwargs)
        
        # Check for errors
        if isinstance(result, dict) and "errors" in result:
            errors = result. get("errors", [])
            error_codes = [err.get("code") for err in errors]
            
            if "QuotaExceeded" in error_codes or "RequestThrottled" in error_codes: 
                if attempt < max_retries - 1:
                    print(f"⏳ Rate limit hit - Retry {attempt + 1}/{max_retries} after {delay:.1f}s")
                    time.sleep(delay)
                    delay *= 2  # Exponential backoff
                    continue
                else:
                    print(f"❌ Failed after {max_retries} retries:  {errors}")
                    return result
            else:
                # Other errors, don't retry
                return result
        
        # Success
        return result
    
    return result


# ---------------------------------------------------------
# Orders + items retrieval (with rate limiting)
# ---------------------------------------------------------
def retrieve_orders_list(method, path, params):
    """Retrieve all orders with rate limiting"""
    all_orders = []
    
    # First request
    orders_rate_limiter.acquire()
    resp = spapi_request(method=method, path=path, params=params)
    
    if "errors" in resp:
        print(f"[orders] Error:  {resp. get('errors')}")
        return all_orders

    payload = resp.get("payload") or {}
    all_orders.extend(payload.get("Orders", []))
    next_token = payload.get("NextToken")

    # Paginate
    while next_token:
        orders_rate_limiter.acquire()
        resp = spapi_request(method=method, path=path, params={"NextToken": next_token})
        
        if "errors" in resp:
            print(f"[orders] Pagination error: {resp.get('errors')}")
            break
            
        payload = resp.get("payload") or {}
        all_orders.extend(payload.get("Orders", []))
        next_token = payload.get("NextToken")

    return all_orders


def get_single_order_items(order_id):
    """Get order items with rate limiting and retry"""
    
    def _fetch():
        order_items_rate_limiter. acquire()
        item_path = f"/orders/v0/orders/{order_id}/orderItems"
        return spapi_request("GET", item_path)
    
    resp = retry_api_call(_fetch)
    
    if "errors" in resp:
        return []
    
    payload = resp. get("payload") or {}
    items = payload.get("OrderItems", [])
    next_token = payload.get("NextToken")

    # Paginate
    while next_token:
        order_items_rate_limiter.acquire()
        item_path = f"/orders/v0/orders/{order_id}/orderItems"
        resp = spapi_request("GET", item_path, params={"NextToken": next_token})
        
        if "errors" in resp:
            print(f"[orderItems] {order_id} pagination error: {resp.get('errors')}")
            break
            
        payload = resp.get("payload") or {}
        items.extend(payload.get("OrderItems", []))
        next_token = payload.get("NextToken")

    return items


def get_order_refunds(order_id):
    """Get refund information with rate limiting and retry"""
    
    def _fetch():
        financials_rate_limiter.acquire()
        refund_path = f"/finances/v0/orders/{order_id}/financialEvents"
        return spapi_request("GET", refund_path, params={})
    
    resp = retry_api_call(_fetch)
    
    refund_amount = 0.0
    
    if "payload" in resp: 
        financial_events = resp["payload"].get("FinancialEvents", {})
        refund_events = financial_events.get("RefundEventList", [])
        
        for event in refund_events: 
            for item_adj in event.get("ShipmentItemAdjustmentList", []):
                for charge in item_adj.get("ItemChargeAdjustmentList", []):
                    charge_type = charge.get("ChargeType")
                    if charge_type in ("Principal", "Tax", "ShippingCharge", "GiftWrap"):
                        amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0) or 0
                        try:
                            refund_amount += abs(float(amount))
                        except Exception:
                            continue
    
    return refund_amount


def get_order_data_combined(order_id):
    """Get both items and refunds for an order"""
    items = get_single_order_items(order_id)
    refund_amount = get_order_refunds(order_id)
    return items, refund_amount


async def get_order_data_batch_async(order_ids):
    """Fetch order data in parallel with controlled concurrency"""
    loop = asyncio.get_event_loop()
    
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, get_order_data_combined, oid) 
            for oid in order_ids
        ]
        results = await asyncio.gather(*tasks)

    order_items_map:  Dict[str, list] = {}
    refund_amount_map: Dict[str, float] = {}

    for oid, (items, refund_amount) in zip(order_ids, results):
        order_items_map[oid] = items
        refund_amount_map[oid] = refund_amount

    return order_items_map, refund_amount_map


# ---------------------------------------------------------
# Fee estimation helpers
# ---------------------------------------------------------
def estimate_fees_for_item(sku, asin, price, cache, counters):
    cache_key = (sku, asin, price)
    if cache_key in cache:
        counters["mem_hits"] += 1
        return cache[cache_key]
    counters["mem_misses"] += 1

    conn = connect_database()
    cursor = conn.cursor()
    try:
        db_entry = get_fee_estimate_from_cache(cursor, sku)
        if db_entry: 
            last_price = db_entry["last_price"]
            fees = db_entry["fees"]
            updated_at = db_entry["updated_at"]
            not_expired = datetime.utcnow() - updated_at <= timedelta(days=FEE_CACHE_TTL_DAYS)
            if last_price == price and not_expired:
                counters["db_hits"] += 1
                cache[cache_key] = fees
                return fees
            else: 
                counters["db_misses"] += 1
        else: 
            counters["db_misses"] += 1

        counters["sp_calls"] += 1
        fees_estimate = get_fees_estimate(sku=sku, asin=asin, price=price)
        if fees_estimate:
            upsert_fee_estimate_cache(cursor, sku, asin, price, fees_estimate)
            conn.commit()
            cache[cache_key] = fees_estimate
        return fees_estimate
    finally: 
        try:
            cursor.close()
        except Exception:
            pass
        try:
            conn.close()
        except Exception:
            pass


async def estimate_fees_batch_async(items_to_estimate, fees_cache, counters):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [
            loop.run_in_executor(executor, estimate_fees_for_item, sku, asin, price, fees_cache, counters)
            for sku, asin, price in items_to_estimate
        ]
        return await asyncio.gather(*tasks)


# ---------------------------------------------------------
# Main orders processor (returns flat list of transactions)
# ---------------------------------------------------------
async def get_orders_async(params, db_cursor=None):
    start_time = time.time()
    
    # 1) Retrieve orders
    print("🔄 Fetching orders...")
    orders = retrieve_orders_list("GET", "/orders/v0/orders", params)
    if not orders:
        print("=" * 60)
        print("📊 ORDERS & TRANSACTIONS SUMMARY")
        print("=" * 60)
        print(f"Total Orders: 0")
        print(f"Total Transactions: 0")
        print("=" * 60)
        return []

    print(f"✅ Retrieved {len(orders)} orders")

    # 2) Items + refunds
    print(f"🔄 Fetching order items and refunds...")
    print(f"⏱️  This will take approximately {len(orders) / 0.5 / 60:.1f} minutes due to API rate limits")
    
    order_ids = [o["AmazonOrderId"] for o in orders]
    order_items_map, refund_amount_map = await get_order_data_batch_async(order_ids)

    # Count successful retrievals
    successful_items = sum(1 for items in order_items_map.values() if items)
    print(f"✅ Successfully retrieved items for {successful_items}/{len(orders)} orders")

    # 3) SKUs
    all_skus = list({
        item["SellerSKU"]
        for items in order_items_map.values()
        for item in items
        if "SellerSKU" in item
    })

    # 4) DB mapping and details
    product_mapping = get_product_mapping(db_cursor, all_skus) if db_cursor else {}
    all_asins = list({m["asin"] for m in product_mapping.values() if "asin" in m})
    asin_details = get_product_details_by_asin(db_cursor, all_asins) if db_cursor else {}

    # 5) Prep fee list
    items_to_estimate = []
    item_metadata = []
    skipped = {"no_items": 0, "no_sku": 0, "no_mapping": 0, "no_asin": 0}

    for order in orders:
        oid = order["AmazonOrderId"]
        order_status = order. get("OrderStatus", "Unknown")
        items = order_items_map.get(oid, [])
        if not items:
            skipped["no_items"] += 1
        refund_amount = refund_amount_map.get(oid, 0.0)

        for item in items:
            sku = item. get("SellerSKU")
            if not sku: 
                skipped["no_sku"] += 1
                continue
            mapping = product_mapping.get(sku)
            if not mapping:
                skipped["no_mapping"] += 1
                continue
            asin = mapping.get("asin")
            if not asin or asin == "Not Available":
                skipped["no_asin"] += 1
                continue

            price_raw = item.get("ItemPrice", {}).get("Amount") or item.get("ItemPrice", {}).get("CurrencyAmount")
            try:
                price = float(price_raw or 0)
            except Exception: 
                price = 0.0

            qty = item.get("QuantityOrdered", 1)
            if not qty or qty <= 0:
                qty = 1

            if price > 0:
                items_to_estimate.append((sku, asin, price))

            item_metadata.append({
                "order_id": oid,
                "order_status": order_status,
                "item":  item,
                "quantity": qty,
                "price":  price,
                "sku": sku,
                "asin": asin,
                "mapping": mapping,
                "refund_amount": refund_amount,
            })

    # 6) Deduplicate fee requests
    unique_items = list({(sku, asin, price) for (sku, asin, price) in items_to_estimate})

    # 7) Fee estimation
    print(f"🔄 Estimating fees for {len(unique_items)} unique items...")
    fees_cache:  Dict[Tuple[str, str, float], Dict] = {}
    counters = {"mem_hits": 0, "mem_misses": 0, "db_hits": 0, "db_misses": 0, "sp_calls": 0}
    unique_fee_estimates = await estimate_fees_batch_async(unique_items, fees_cache, counters)
    fees_by_key = {(sku, asin, price): fees for (sku, asin, price), fees in zip(unique_items, unique_fee_estimates)}

    # 8) Build flat transactions
    transactions:  List[Dict[str, Any]] = []
    for meta in item_metadata:
        oid = meta["order_id"]
        item = meta["item"]
        qty = meta["quantity"]
        price = meta["price"]
        sku = meta["sku"]
        asin = meta["asin"]
        mapping = meta["mapping"]
        refund_amount = meta["refund_amount"]
        order_status = meta["order_status"]

        fees = fees_by_key.get((sku, asin, price)) if price > 0 else None
        currency = item.get("ItemPrice", {}).get("CurrencyCode", BASE_CURRENCY_CODE)

        for _ in range(qty):
            t = {
                "AmazonOrderId": oid,
                "OrderStatus": order_status,
                "Refunded": "Yes" if refund_amount != 0 else "No",
                "RefundedAmount": refund_amount,
                "SKU": sku,
                "ASIN": asin,
                "SSKU": mapping.get("ssku", "Not Available"),
                "Currency": currency,
                "SOLD":  price,
            }

            details = asin_details.get(asin, {})
            t["Brand"] = details.get("brand", "Not Available")
            t["Category"] = details.get("category", "Not Available")

            if fees:
                referral_fee_wo_vat = fees.get("ReferralFees", 0) or 0
                fba_fee_wo_vat = fees.get("FBAFees", 0) or 0
                total_fees_wo_vat = referral_fee_wo_vat + fba_fee_wo_vat

                referral_fee_w_vat = referral_fee_wo_vat * AMAZON_VAT_MULTIPLIER
                fba_fee_w_vat = fba_fee_wo_vat * AMAZON_VAT_MULTIPLIER
                total_fees_w_vat = referral_fee_w_vat + fba_fee_w_vat

                amazon_vat_amount = total_fees_w_vat - total_fees_wo_vat

                t["Est Fee"] = -referral_fee_w_vat
                t["Est FBAFees"] = -fba_fee_w_vat
                t["Est TotalAmazonFees"] = -total_fees_w_vat
                t["Est R. VAT"] = amazon_vat_amount
                t["Est Fee%"] = (referral_fee_wo_vat / price) * 100 if price > 0 else 0

                government_vat_amount = price * GOVT_VAT_RATE
                t["VAT"] = -government_vat_amount

                cost = parse_cost(details.get("cost"))
                t["COG"] = -cost if cost is not None else "Not Available"

                if currency == BASE_CURRENCY_CODE and cost is not None:
                    net_profit = (
                        price
                        - total_fees_w_vat
                        - government_vat_amount
                        + amazon_vat_amount
                        - cost
                    )
                    t["Est Net Profit"] = net_profit
                else:
                    t["Est Net Profit"] = "Not Available"
            else:
                t["Est Fee"] = "Not Available"
                t["Est FBAFees"] = "Not Available"
                t["Est TotalAmazonFees"] = "Not Available"
                t["Est R.VAT"] = "Not Available"
                t["Est Fee%"] = "Not Available"
                t["VAT"] = -(price * GOVT_VAT_RATE)
                cost = parse_cost(details.get("cost"))
                t["COG"] = -cost if cost else "Not Available"
                t["Est Net Profit"] = "Not Available"

            transactions.append(t)

    # Commit DB
    try:
        if db_cursor:
            db_cursor.connection.commit()
    except Exception:
        pass

    # Print summary
    elapsed_time = time.time() - start_time
    print("=" * 60)
    print("📊 ORDERS & TRANSACTIONS SUMMARY")
    print("=" * 60)
    print(f"Total Orders: {len(orders)}")
    print(f"Total Transactions: {len(transactions)}")
    print(f"Orders with items: {successful_items}")
    print(f"Orders with no items: {skipped['no_items']}")
    print(f"Items skipped (no SKU): {skipped['no_sku']}")
    print(f"Items skipped (no mapping): {skipped['no_mapping']}")
    print(f"Items skipped (no ASIN): {skipped['no_asin']}")
    print(f"⏱️  Total processing time: {elapsed_time / 60:.1f} minutes")
    print("=" * 60)

    return transactions


async def get_orders(params, db_cursor=None):
    return await get_orders_async(params, db_cursor)