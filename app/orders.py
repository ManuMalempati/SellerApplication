import os
import io
import csv
import zlib
import httpx
import time
import asyncio
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple, Dict

from .auth import spapi_request
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
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "16"))
FEE_CACHE_TTL_DAYS = int(os.getenv("FEE_CACHE_TTL_DAYS", "7"))
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

# ---------------------------------------------------------
# Orders + items retrieval
# ---------------------------------------------------------
def retrieve_orders_list(method, path, params):
    all_orders = []
    json_response = spapi_request(method=method, path=path, params=params)
    if "errors" in json_response:
        return all_orders

    payload = json_response.get("payload")
    if not payload:
        return all_orders

    def extract_orders(p):
        orders = p.get("Orders", [])
        all_orders.extend(orders)

    extract_orders(payload)
    next_token = payload.get("NextToken")

    while next_token:
        json_response = spapi_request(method=method, path=path, params={"NextToken": next_token})
        if "errors" in json_response:
            break
        payload = json_response.get("payload")
        if not payload:
            break
        extract_orders(payload)
        next_token = payload.get("NextToken")

    return all_orders

def get_single_order_items(order_id):
    item_path = f"/orders/v0/orders/{order_id}/orderItems"
    response = spapi_request("GET", item_path)
    if "errors" in response:
        return []

    payload = response.get("payload")
    if not payload:
        return []

    items = payload.get("OrderItems", [])
    next_token = payload.get("NextToken")

    while next_token:
        response = spapi_request("GET", item_path, params={"NextToken": next_token})
        if "errors" in response:
            break
        payload = response.get("payload")
        if not payload:
            break
        items.extend(payload.get("OrderItems", []))
        next_token = payload.get("NextToken")

    return items

# ---------------------------------------------------------
# Items + refunds per order
# ---------------------------------------------------------
def get_order_data_combined(order_id):
    items = get_single_order_items(order_id)

    # Refunds via Finances order endpoint (per SP-API docs)
    refund_amount = 0.0
    refund_path = f"/finances/v0/orders/{order_id}/financialEvents"
    refund_response = spapi_request("GET", refund_path, params={})

    if "payload" in refund_response:
        financial_events = refund_response["payload"].get("FinancialEvents", {})
        refund_events = financial_events.get("RefundEventList", [])
        for event in refund_events:
            for item_adj in event.get("ShipmentItemAdjustmentList", []):
                for charge in item_adj.get("ItemChargeAdjustmentList", []):
                    charge_type = charge.get("ChargeType")
                    # Count principal and common refund-related amounts; partial refunds included
                    if charge_type in ("Principal", "Tax", "ShippingCharge", "GiftWrap"):
                        amount = charge.get("ChargeAmount", {}).get("CurrencyAmount", 0) or 0
                        try:
                            refund_amount += abs(float(amount))
                        except:
                            continue

    is_refunded = refund_amount != 0.0
    return items, is_refunded, refund_amount

async def get_order_data_batch_async(order_ids):
    loop = asyncio.get_event_loop()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        tasks = [loop.run_in_executor(executor, get_order_data_combined, order_id) for order_id in order_ids]
        results = await asyncio.gather(*tasks)

    order_items_map: Dict[str, list] = {}
    refund_status_map: Dict[str, bool] = {}
    refund_amount_map: Dict[str, float] = {}

    for order_id, (items, refunded, refund_amount) in zip(order_ids, results):
        order_items_map[order_id] = items
        refund_status_map[order_id] = refunded
        refund_amount_map[order_id] = refund_amount

    return order_items_map, refund_status_map, refund_amount_map

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
        except:
            pass
        try:
            conn.close()
        except:
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
# Main orders processor
# ---------------------------------------------------------
async def get_orders_async(params, db_cursor):
    overall_start = time.perf_counter()
    step_times: Dict[str, float] = {}

    counters = {"mem_hits": 0, "mem_misses": 0, "db_hits": 0, "db_misses": 0, "sp_calls": 0}

    # Step 1: Retrieve orders
    t0 = time.perf_counter()
    orders = retrieve_orders_list("GET", "/orders/v0/orders", params)
    step_times["Step 1"] = time.perf_counter() - t0
    if not orders:
        return []

    # Step 2: Items + refunds
    t0 = time.perf_counter()
    order_ids = [o["AmazonOrderId"] for o in orders]
    order_items_map, refund_status_map, refund_amount_map = await get_order_data_batch_async(order_ids)
    step_times["Step 2"] = time.perf_counter() - t0

    # Step 3: Extract SKUs
    t0 = time.perf_counter()
    all_skus = list({
        item["SellerSKU"]
        for items in order_items_map.values()
        for item in items
        if "SellerSKU" in item
    })
    step_times["Step 3"] = time.perf_counter() - t0

    # Step 4: DB product mapping
    t0 = time.perf_counter()
    product_mapping = get_product_mapping(db_cursor, all_skus)
    step_times["Step 4"] = time.perf_counter() - t0

    # Step 5: Extract ASINs
    t0 = time.perf_counter()
    all_asins = list({
        mapping["asin"]
        for mapping in product_mapping.values()
        if "asin" in mapping
    })
    step_times["Step 5"] = time.perf_counter() - t0

    # Step 6: DB product details
    t0 = time.perf_counter()
    asin_details = get_product_details_by_asin(db_cursor, all_asins)
    step_times["Step 6"] = time.perf_counter() - t0

    # Step 7: Prepare fee list (log skips)
    t0 = time.perf_counter()
    items_to_estimate = []
    item_metadata = []
    skipped = {"no_items": 0, "no_sku": 0, "no_mapping": 0, "no_asin": 0, "bad_price": 0}

    for order in orders:
        order_id = order["AmazonOrderId"]
        items = order_items_map.get(order_id, [])
        if not items:
            skipped["no_items"] += 1
        refund_amount = refund_amount_map.get(order_id, 0.0)

        for item in items:
            sku = item.get("SellerSKU")
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
            except:
                price = 0.0
            if price <= 0:
                skipped["bad_price"] += 1
                continue

            items_to_estimate.append((sku, asin, price))
            item_metadata.append({
                "order_id": order_id,
                "item": item,
                "quantity": item.get("QuantityOrdered", 1),
                "price": price,
                "sku": sku,
                "asin": asin,
                "mapping": mapping,
                "refund_amount": refund_amount,
            })
    step_times["Step 7"] = time.perf_counter() - t0

    # Step 8: Deduplicate fee requests
    t0 = time.perf_counter()
    unique_items = list({(sku, asin, price) for (sku, asin, price) in items_to_estimate})
    step_times["Step 8"] = time.perf_counter() - t0

    # Step 9: Fee estimation
    t0 = time.perf_counter()
    fees_cache: Dict[Tuple[str, str, float], Dict] = {}
    unique_fee_estimates = await estimate_fees_batch_async(unique_items, fees_cache, counters)
    step_times["Step 9"] = time.perf_counter() - t0

    fees_by_key = {(sku, asin, price): fees for (sku, asin, price), fees in zip(unique_items, unique_fee_estimates)}

    # Step 10: Build transactions
    t0 = time.perf_counter()
    transactions = []

    for meta in item_metadata:
        order_id = meta["order_id"]
        item = meta["item"]
        qty = meta["quantity"]
        price = meta["price"]
        sku = meta["sku"]
        asin = meta["asin"]
        mapping = meta["mapping"]
        refund_amount = meta["refund_amount"]

        fees = fees_by_key.get((sku, asin, price))
        currency = item.get("ItemPrice", {}).get("CurrencyCode", BASE_CURRENCY_CODE)

        for _ in range(qty):
            t = {
                "AmazonOrderId": order_id,
                "Refunded": "Yes" if refund_amount != 0 else "No",
                "RefundedAmount": refund_amount,
                "SKU": sku,
                "ASIN": asin,
                "SSKU": mapping.get("ssku", "Not Available"),
            }

            details = asin_details.get(asin, {})
            t["Brand"] = details.get("brand", "Not Available")
            t["Category"] = details.get("category", "Not Available")

            t["Currency"] = currency
            t["SOLD"] = price

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
                t["Est R.VAT"] = amazon_vat_amount
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

    step_times["Step 10"] = time.perf_counter() - t0

    # Commit DB
    try:
        db_cursor.connection.commit()
    except:
        pass

    # Summary logs
    print("\n============================================================")
    print("ORDERS API - Performance Summary")
    print("============================================================")
    print(f"Total orders retrieved: {len(orders)}")
    print(f"Total transactions built: {len(transactions)}")
    print(f"Unique SKUs processed: {len(set([m['sku'] for m in item_metadata]))}")
    print(f"Unique ASINs processed: {len(set([m['asin'] for m in item_metadata]))}")
    print(f"Unique fee estimates requested: {len({(sku, asin, price) for (sku, asin, price) in items_to_estimate})}")
    print("------------------------------------------------------------")
    print("Step Timings:")
    for k, v in step_times.items():
        print(f"  {k}: {v:.3f}s")
    print("------------------------------------------------------------")
    print("Fee Cache Statistics:")
    print(f"  Memory hits: {counters['mem_hits']}")
    print(f"  Memory misses: {counters['mem_misses']}")
    print(f"  DB hits: {counters['db_hits']}")
    print(f"  DB misses: {counters['db_misses']}")
    print(f"  SP-API calls: {counters['sp_calls']}")
    print("------------------------------------------------------------")
    print(f"Skipped summary: {skipped}")
    print("============================================================\n")

    return transactions

async def get_orders(params, db_cursor):
    """Async wrapper for get_orders_async."""
    return await get_orders_async(params, db_cursor)