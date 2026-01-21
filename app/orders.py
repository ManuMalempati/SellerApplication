# orders.py
import os
import asyncio
from concurrent.futures import ThreadPoolExecutor
from .auth import spapi_request
from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost
)
from .estimates import get_fees_estimate

GOVT_VAT_RATE = 1/float(os.getenv("GOVT_VAT_RATE_DIVISOR"))
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")

def retrieve_orders_list(method, path, params):
    """
    Get all orders through pagination
    
    Args:
        method: HTTP method
        path: API endpoint path
        params: Query parameters
    
    Returns:
        list: All orders
    """
    all_orders = []
    
    # Initial API call to get orders
    json_response = spapi_request(method=method, path=path, params=params)
    
    if "errors" in json_response:
        return all_orders
    
    payload = json_response.get("payload")
    if not payload:
        return all_orders
    
    # Extract orders from payload
    def extract_orders(payload):
        orders = payload.get("Orders", [])
        all_orders.extend(orders)
    
    # Extract orders from initial payload
    extract_orders(payload)
    
    # Paginate through remaining orders
    next_token = payload.get("NextToken")
    
    while next_token:
        json_response = spapi_request(
            method=method,
            path=path,
            params={"NextToken": next_token}
        )
        
        if "errors" in json_response:
            break
        
        payload = json_response.get("payload")
        if not payload:
            break
        
        extract_orders(payload)
        next_token = payload.get("NextToken")
    
    return all_orders


def get_single_order_items(order_id):
    """
    Get items for a single order
    
    Args:
        order_id: Amazon Order ID
    
    Returns:
        list: Order items
    """
    item_path = f"/orders/v0/orders/{order_id}/orderItems"
    item_response = spapi_request(method="GET", path=item_path)
    
    if "errors" in item_response:
        return []
    
    item_payload = item_response.get("payload")
    if not item_payload:
        return []
    
    # Get order items
    items = item_payload.get("OrderItems", [])
    
    # Paginate through order items if there are more
    item_next_token = item_payload.get("NextToken")
    while item_next_token:
        item_response = spapi_request(
            method="GET",
            path=item_path,
            params={"NextToken": item_next_token}
        )
        
        if "errors" in item_response:
            break
        
        item_payload = item_response.get("payload")
        if not item_payload:
            break
        
        items.extend(item_payload.get("OrderItems", []))
        item_next_token = item_payload.get("NextToken")
    
    return items


def get_single_order_financial_events(order_id):
    """
    Get financial events for a single order to check for refunds
    
    Args:
        order_id: Amazon Order ID
    
    Returns:
        bool: True if order has refunds, False otherwise
    """
    financial_path = f"/finances/v0/orders/{order_id}/financialEvents"
    financial_response = spapi_request(method="GET", path=financial_path, params={})
    
    if "errors" in financial_response:
        return False
    
    payload = financial_response.get("payload")
    if not payload:
        return False
    
    financial_events = payload.get("FinancialEvents", {})
    
    # Check if RefundEventList exists and is not empty
    refund_events = financial_events.get("RefundEventList", [])
    
    return len(refund_events) > 0


async def get_order_items_batch_async(order_ids):
    """
    Get order items for multiple orders in parallel
    
    Args:
        order_ids: List of Amazon Order IDs
    
    Returns:
        dict: Mapping of order_id -> list of order items
    """
    loop = asyncio.get_event_loop()
    
    # Use ThreadPoolExecutor to run blocking I/O in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create tasks for all orders
        tasks = [
            loop.run_in_executor(executor, get_single_order_items, order_id)
            for order_id in order_ids
        ]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
    
    # Map results back to order_ids
    order_items_map = {
        order_id: items 
        for order_id, items in zip(order_ids, results)
    }
    
    return order_items_map


async def get_order_refund_status_batch_async(order_ids):
    """
    Get refund status for multiple orders in parallel
    
    Args:
        order_ids: List of Amazon Order IDs
    
    Returns:
        dict: Mapping of order_id -> bool (True if refunded, False otherwise)
    """
    loop = asyncio.get_event_loop()
    
    # Use ThreadPoolExecutor to run blocking I/O in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        # Create tasks for all orders
        tasks = [
            loop.run_in_executor(executor, get_single_order_financial_events, order_id)
            for order_id in order_ids
        ]
        
        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
    
    # Map results back to order_ids
    refund_status_map = {
        order_id: is_refunded 
        for order_id, is_refunded in zip(order_ids, results)
    }
    
    return refund_status_map


def estimate_fees_for_item(sku, asin, price, cache):
    """
    Estimate fees with caching
    
    Args:
        sku: Seller SKU
        asin: ASIN
        price: Item price
        cache: Fee cache dictionary
    
    Returns:
        dict or None: Fee estimate
    """
    cache_key = f"{sku}_{asin}_{price}"
    
    if cache_key in cache:
        return cache[cache_key]
    
    fees_estimate = get_fees_estimate(sku=sku, asin=asin, price=price)
    cache[cache_key] = fees_estimate
    
    return fees_estimate


async def estimate_fees_batch_async(items_to_estimate, fees_cache):
    """
    Estimate fees for multiple items in parallel
    
    Args:
        items_to_estimate: List of (sku, asin, price) tuples
        fees_cache: Shared fee cache dictionary
    
    Returns:
        list: List of fee estimates in same order as input
    """
    loop = asyncio.get_event_loop()
    
    # Use ThreadPoolExecutor to run blocking I/O in parallel
    with ThreadPoolExecutor(max_workers=10) as executor:
        tasks = [
            loop.run_in_executor(
                executor, 
                estimate_fees_for_item, 
                sku, 
                asin, 
                price, 
                fees_cache
            )
            for sku, asin, price in items_to_estimate
        ]
        
        results = await asyncio.gather(*tasks)
    
    return results


async def get_orders_async(params, db_cursor):
    """
    Process orders and return transaction details with estimated fees (async version)
    
    Args:
        params: Query parameters for API call
        db_cursor: Database cursor
    
    Returns:
        list: List of transaction dictionaries
    """
    method = "GET"
    path = "/orders/v0/orders"
    
    # Step 1: Retrieve all orders from API
    orders = retrieve_orders_list(method, path, params)
    
    if not orders:
        return []
    
    # Step 2: Get all order items and refund status in parallel
    order_ids = [order["AmazonOrderId"] for order in orders]
    
    # Run both tasks concurrently
    order_items_map, refund_status_map = await asyncio.gather(
        get_order_items_batch_async(order_ids),
        get_order_refund_status_batch_async(order_ids)
    )
    
    # Step 3: Extract all unique seller SKUs from all order items
    all_seller_skus = list(set([
        item["SellerSKU"] 
        for items in order_items_map.values()
        for item in items
        if "SellerSKU" in item
    ]))
    
    if not all_seller_skus:
        return []
    
    # Step 4: Get complete mapping: SKU -> {ASIN, SSKU}
    product_mapping = get_product_mapping(db_cursor, all_seller_skus)
    
    # Step 5: Extract all ASINs from the mapping
    all_asins = list(set([
        mapping["asin"] 
        for mapping in product_mapping.values()
        if "asin" in mapping
    ]))
    
    # Step 6: Get product details for all ASINs (cost, brand, category)
    asin_details = get_product_details_by_asin(db_cursor, all_asins)
    
    # Step 7: Prepare all items that need fee estimation
    items_to_estimate = []
    item_metadata = []  # Store metadata to reconstruct transactions later
    
    for order in orders:
        order_id = order["AmazonOrderId"]
        items = order_items_map.get(order_id, [])
        is_refunded = refund_status_map.get(order_id, False)
        
        for item in items:
            # Skip items without SellerSKU
            if "SellerSKU" not in item:
                continue
                
            quantity_ordered = item.get("QuantityOrdered", 1)
            
            # Get item price per unit (ItemPrice is already per unit)
            item_price_obj = item.get("ItemPrice", {})
            
            # Handle both string and numeric amounts
            item_price_amount = item_price_obj.get("Amount", 0)
            try:
                item_price = float(item_price_amount) if item_price_amount else 0
            except (ValueError, TypeError):
                item_price = 0
            
            sku = item["SellerSKU"]
            mapping = product_mapping.get(sku, {})
            asin = mapping.get("asin", "Not Available")
            
            # Only estimate fees if we have valid SKU and ASIN
            if sku and asin != "Not Available" and item_price > 0:
                # Add to estimation list
                items_to_estimate.append((sku, asin, item_price))
                
                # Store metadata for later
                item_metadata.append({
                    "order_id": order_id,
                    "item": item,
                    "quantity_ordered": quantity_ordered,
                    "item_price": item_price,
                    "sku": sku,
                    "asin": asin,
                    "mapping": mapping,
                    "is_refunded": is_refunded
                })
    
    # Step 8: Estimate fees for all items in parallel
    fees_cache = {}
    fee_estimates = await estimate_fees_batch_async(items_to_estimate, fees_cache)
    
    # Step 9: Build transactions using estimated fees
    transactions = []
    
    for metadata, fees_estimate in zip(item_metadata, fee_estimates):
        order_id = metadata["order_id"]
        item = metadata["item"]
        quantity_ordered = metadata["quantity_ordered"]
        item_price = metadata["item_price"]
        sku = metadata["sku"]
        asin = metadata["asin"]
        mapping = metadata["mapping"]
        is_refunded = metadata["is_refunded"]
        
        # Process each unit separately
        for i in range(quantity_ordered):
            transaction = {}
            
            # Basic identifiers
            transaction["AmazonOrderId"] = order_id
            transaction["Refunded"] = "Yes" if is_refunded else "No"
            transaction["SKU"] = sku
            transaction["ASIN"] = asin
            transaction["SSKU"] = mapping.get("ssku", "Not Available")
            
            # Get product details
            details = asin_details.get(asin, {})
            transaction["Brand"] = details.get("brand", "Not Available")
            transaction["Category"] = details.get("category", "Not Available")
            
            # Currency
            item_price_obj = item.get("ItemPrice", {})
            currency_code = item_price_obj.get("CurrencyCode", BASE_CURRENCY_CODE)
            transaction["Currency"] = currency_code
            
            # Item price (already per unit)
            transaction["SOLD"] = item_price
            
            # Fees
            if fees_estimate:
                total_fees = fees_estimate.get("TotalAmazonFees", 0)
                referral_fee = fees_estimate.get("ReferralFees", 0)
                fba_fees = fees_estimate.get("FBAFees", 0)
                
                transaction["Est Fee"] = -referral_fee
                transaction["Est FBAFees"] = -fba_fees
                transaction["Est TotalAmazonFees"] = -total_fees
                
                # VAT calculations - ONLY for Fee and FBAFees
                # R.VAT = (Fee + FBAFees) / 21
                fees_with_vat = referral_fee + fba_fees
                fees_vat = fees_with_vat / (1 / GOVT_VAT_RATE)
                
                transaction["Est R.VAT"] = fees_vat
                
                # Calculate referral VAT for Fee% calculation
                referral_vat = referral_fee / (1 / GOVT_VAT_RATE)
                
                # Referral Fee % Charged by Amazon (without VAT component)
                if item_price != 0:
                    net_referral_fee = referral_fee - referral_vat
                    transaction["Est Fee%"] = (net_referral_fee / item_price) * 100
                else:
                    transaction["Est Fee%"] = 0
            else:
                # If fees estimation fails, mark as unavailable
                transaction["Est Fee"] = "Not Available"
                transaction["Est FBAFees"] = "Not Available"
                transaction["Est TotalAmazonFees"] = "Not Available"
                transaction["Est R.VAT"] = "Not Available"
                transaction["Est Fee%"] = "Not Available"
                fees_vat = 0
                total_fees = 0
            
            # Calculate government VAT (% of item price, negative)
            vat_amount = item_price * GOVT_VAT_RATE
            transaction["VAT"] = -vat_amount
            
            # Get and parse cost
            cost = parse_cost(details.get("cost"))
            
            # Store cost of goods (make negative to show money out)
            # ALWAYS IN AED SINCE WE FETCH FROM DB
            if cost is None:
                transaction["COG"] = "Not Available"
            else:
                transaction["COG"] = -cost
            
            # Calculate net profit
            # Net Profit = SOLD - Amazon Fees - Item VAT + R.VAT - COG
            if currency_code == BASE_CURRENCY_CODE and cost is not None and fees_estimate:
                net_profit = item_price - total_fees - vat_amount + fees_vat - cost
                transaction["Est Net Profit"] = net_profit
            else:
                transaction["Est Net Profit"] = "Not Available"
            
            transactions.append(transaction)
    
    return transactions

async def get_orders(params, db_cursor):
    return await get_orders_async(params, db_cursor)