import json
import os
import time
import threading
from . database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost
)
from .auth import spapi_request

BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
GOVT_VAT_RATE = 1/float(os.getenv("GOVT_VAT_RATE_DIVISOR"))

# Retry settings
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


# ---------------------------------------------------------
# Token Bucket Rate Limiter
# ---------------------------------------------------------
class TokenBucketRateLimiter: 
    """Thread-safe token bucket rate limiter"""
    
    def __init__(self, rate:  float, burst: int):
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
                elapsed = now - self. last_update
                
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


# Initialize rate limiter for listTransactions endpoint
# Rate:  0.5 req/s, Burst: 10
transactions_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=10)


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
                print(f"⚠️  API Error:  {errors}")
                return result
        
        # Success
        return result
    
    return result


def retrieve_transaction_list(method, path, params):
    """
    Get all transactions through pagination (listTransactions API) with rate limiting
    
    Args:
        method: HTTP method
        path: API endpoint path
        params: Query parameters
    
    Returns:
        list: All transactions
    """
    all_transactions = []
    page_count = 0
    
    # Initial API call with rate limiting
    def _fetch_initial():
        transactions_rate_limiter.acquire()
        return spapi_request(method=method, path=path, params=params)
    
    json_response = retry_api_call(_fetch_initial)
    
    if "errors" in json_response: 
        print(f"[transactions] Error on initial request: {json_response. get('errors')}")
        return all_transactions
    
    payload = json_response.get("payload")
    if not payload:
        return all_transactions
    
    # Extract transactions from payload
    transactions = payload.get("transactions", [])
    all_transactions.extend(transactions)
    page_count += 1
    print(f"📄 Page {page_count}:  Retrieved {len(transactions)} transactions")
    
    # Paginate through remaining results
    next_token = payload.get("nextToken")
    
    while next_token:
        def _fetch_page():
            transactions_rate_limiter.acquire()
            return spapi_request(
                method=method,
                path=path,
                params={"nextToken": next_token}
            )
        
        json_response = retry_api_call(_fetch_page)
        
        if "errors" in json_response: 
            print(f"[transactions] Error on page {page_count + 1}: {json_response. get('errors')}")
            break
        
        payload = json_response.get("payload")
        if not payload:
            break
        
        transactions = payload.get("transactions", [])
        all_transactions.extend(transactions)
        page_count += 1
        print(f"📄 Page {page_count}: Retrieved {len(transactions)} transactions")
        
        next_token = payload. get("nextToken")
    
    print(f"✅ Total pages retrieved: {page_count}")
    print(f"✅ Total transactions from API: {len(all_transactions)}")
    
    return all_transactions


def extract_breakdown_value(breakdowns, breakdown_type, sub_type=None):
    """
    Extract specific breakdown amount from nested breakdowns structure
    
    Args:
        breakdowns: List of breakdown objects
        breakdown_type: Type to search for (e.g., "Commission", "FBAPerUnitFulfillmentFee")
        sub_type: Optional sub-type to search for (e.g., "Base", "Tax")
    
    Returns:
        float: The breakdown amount, or 0 if not found
    """
    if not breakdowns:
        return 0
    
    for breakdown in breakdowns:
        if breakdown. get("breakdownType") == breakdown_type:
            # If we need a sub-type (like "Base" or "Tax")
            if sub_type: 
                nested = breakdown.get("breakdowns", [])
                for nested_breakdown in nested:
                    if nested_breakdown.get("breakdownType") == sub_type:
                        amount = nested_breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                        return abs(amount) if amount else 0
            else:
                # Return the main breakdown amount
                amount = breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                return abs(amount) if amount else 0
        
        # Recursively search nested breakdowns
        nested = breakdown.get("breakdowns")
        if nested:
            result = extract_breakdown_value(nested, breakdown_type, sub_type)
            if result != 0:
                return result
    
    return 0


def calculate_fees_from_transaction(item):
    """
    Calculate all fees for a transaction item (new API structure)
    
    Args:
        item: Transaction item data
    
    Returns:
        dict: Fee breakdown with base amounts and VAT separated
    """
    item_breakdowns = item.get("breakdowns", [])
    
    # Extract Commission (Referral Fee) - Base and Tax separately
    referral_fee_base = extract_breakdown_value(item_breakdowns, "Commission", "Base")
    referral_fee_tax = extract_breakdown_value(item_breakdowns, "Commission", "Tax")
    referral_fee_total = referral_fee_base + referral_fee_tax
    
    # Extract FBA fees - Base and Tax separately
    fba_fee_base = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Base")
    fba_fee_tax = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Tax")
    fba_fee_total = fba_fee_base + fba_fee_tax
    
    # Extract ShippingChargeback (if exists)
    shipping_charge_back_fees = extract_breakdown_value(item_breakdowns, "ShippingChargeback")
    
    # Total fees (all including VAT)
    total_fees = referral_fee_total + fba_fee_total + shipping_charge_back_fees
    
    return {
        "referral_fee_base": referral_fee_base,
        "referral_fee_tax": referral_fee_tax,
        "referral_fee_total":  referral_fee_total,
        "fba_fee_base": fba_fee_base,
        "fba_fee_tax": fba_fee_tax,
        "fba_fee_total": fba_fee_total,
        "shipping_charge_back_fees": shipping_charge_back_fees,
        "total":  total_fees
    }


def calculate_customer_charges_from_transaction(item):
    """
    Calculate customer charges and promotions (new API structure)
    
    Args:
        item: Transaction item data
    
    Returns: 
        dict: Charge breakdown
    """
    item_breakdowns = item.get("breakdowns", [])
    
    # Extract OurPricePrincipal (item price)
    item_price = extract_breakdown_value(item_breakdowns, "OurPricePrincipal")
    
    # Extract Shipping charges
    shipping_charge = extract_breakdown_value(item_breakdowns, "Shipping")
    if shipping_charge == 0:
        shipping_charge = extract_breakdown_value(item_breakdowns, "ShippingPrincipal")
    
    # Extract promotions (PromoRebates or ShippingDiscount)
    total_promotions = extract_breakdown_value(item_breakdowns, "PromoRebates")
    if total_promotions == 0:
        total_promotions = extract_breakdown_value(item_breakdowns, "ShippingDiscount")
    
    # Total charges = item price + shipping
    total_charges = item_price + shipping_charge
    
    # Sales Proceed = Total Charges - Promotions
    sales_proceed = total_charges - total_promotions
    
    return {
        "item_price":  item_price,
        "shipping_charge": shipping_charge,
        "total_charges": total_charges,
        "total_promotions": total_promotions,
        "sales_proceed":  sales_proceed
    }


def calculate_profit(currency_code, sales_proceed, fees_total, vat_amount, fees_vat, cost):
    """
    Calculate net profit
    
    Args:
        currency_code: AED or any other type of currency
        sales_proceed: Sales proceed after promotions
        fees_total: Total fees
        vat_amount:  VAT amount on item price
        fees_vat: VAT on fees (can be claimed back)
        cost: Cost of goods
    
    Returns:
        float or str: Net profit or "Not Available"
    """
    if cost is None:
        return "Not Available"
    # No access to currency exchange API
    elif currency_code != BASE_CURRENCY_CODE: 
        return "Currency Code Different"
    
    # Net Profit = Sales Proceed - Total Fees - Item VAT + Fees VAT - COG
    net_profit = sales_proceed - fees_total - vat_amount + fees_vat - cost
    return net_profit


def get_transactions(params, db_cursor):
    """
    Process financial transactions and return transaction details
    
    Args:
        params: Query parameters for API call
        db_cursor: Database cursor
    
    Returns:
        list: List of transaction dictionaries
    """
    start_time = time.time()
    
    print("=" * 60)
    print("🔄 FETCHING FINANCIAL TRANSACTIONS")
    print("=" * 60)

    method = "GET"
    path = "/finances/2024-06-19/transactions"
    
    # Step 1: Retrieve all transactions from API
    print("📥 Retrieving transactions from Amazon SP-API...")
    api_transactions = retrieve_transaction_list(method, path, params)
    
    if not api_transactions: 
        print("=" * 60)
        print("📊 TRANSACTIONS SUMMARY")
        print("=" * 60)
        print(f"Total API Transactions: 0")
        print(f"Total Processed Transactions: 0")
        print("=" * 60)
        return []
    
    # Step 2: Extract all unique seller SKUs from contexts
    print("🔍 Extracting SKUs from transactions...")
    all_seller_skus = []
    for transaction in api_transactions:
        for item in transaction.get("items", []):
            for context in item.get("contexts", []):
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    if sku: 
                        all_seller_skus.append(sku)
    
    all_seller_skus = list(set(all_seller_skus))
    print(f"✅ Found {len(all_seller_skus)} unique SKUs")
    
    # Step 3: Get complete mapping:  SKU -> {ASIN, SSKU}
    print("🗄️  Fetching product mappings from database...")
    product_mapping = get_product_mapping(db_cursor, all_seller_skus)
    print(f"✅ Retrieved mappings for {len(product_mapping)} SKUs")
    
    # Step 4: Extract all ASINs from the mapping
    all_asins = list(set([
        mapping["asin"] 
        for mapping in product_mapping.values()
        if "asin" in mapping
    ]))
    
    # Step 5: Get product details for all ASINs (cost, brand, category)
    print(f"🗄️  Fetching product details for {len(all_asins)} ASINs...")
    asin_details = get_product_details_by_asin(db_cursor, all_asins)
    print(f"✅ Retrieved details for {len(asin_details)} ASINs")
    
    # Step 6: Process each transaction
    print("⚙️  Processing transactions...")
    transactions = []
    skipped = {"no_sku": 0, "no_mapping": 0}
    
    for api_transaction in api_transactions: 
        # Get transaction type and status
        transaction_type = api_transaction.get("transactionType", "Unknown")
        transaction_status = api_transaction.get("transactionStatus", "Unknown")
        
        # Get order ID from relatedIdentifiers
        order_id = None
        for identifier in api_transaction.get("relatedIdentifiers", []):
            if identifier.get("relatedIdentifierName") == "ORDER_ID":
                order_id = identifier.get("relatedIdentifierValue")
                break
        
        for item in api_transaction.get("items", []):
            # Extract SKU and ASIN from contexts
            sku = None
            asin = None
            for context in item.get("contexts", []):
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    asin = context.get("asin")
                    break
            
            if not sku: 
                skipped["no_sku"] += 1
                continue  # Skip if no SKU found
            
            # Initialize transaction record
            transaction = {}
            
            # Transaction metadata
            transaction["TransactionType"] = transaction_type
            transaction["TransactionStatus"] = transaction_status
            
            # Basic identifiers
            transaction["AmazonOrderId"] = order_id or "Not Available"
            transaction["SKU"] = sku
            
            # Get mapping for this SKU: SKU -> ASIN -> SSKU
            mapping = product_mapping. get(sku, {})
            
            if not mapping:
                skipped["no_mapping"] += 1
            
            transaction["ASIN"] = asin or mapping.get("asin", "Not Available")
            transaction["SSKU"] = mapping.get("ssku", "Not Available")
            
            # Get product details for this ASIN
            asin_key = transaction["ASIN"]
            details = asin_details.get(asin_key, {})
            
            transaction["Brand"] = details.get("brand", "Not Available")
            transaction["Category"] = details.get("category", "Not Available")
            
            # Calculate customer charges, promotions, and sales proceed
            charges = calculate_customer_charges_from_transaction(item)

            # Currency Code might differ
            transaction["Currency"] = item.get("totalAmount", {}).get("currencyCode", BASE_CURRENCY_CODE)
            
            # Item listing price and shipping
            transaction["SOLD"] = charges["item_price"]
            transaction["ShippingCharge"] = charges["shipping_charge"]
            
            # Promotions (negative value - shows discount given)
            transaction["TotalPromotions"] = -charges["total_promotions"]
            
            # Sales Proceed (what customer actually paid after promotions)
            transaction["SalesProceed"] = charges["sales_proceed"]
            
            # Calculate all fees (extracted from API breakdowns)
            fees = calculate_fees_from_transaction(item)

            # Referral Fee was asked to be named as Fee
            transaction["Fee"] = -fees["referral_fee_total"]
            transaction["FBAFees"] = -fees["fba_fee_total"]
            transaction["ShippingChargeback"] = -fees["shipping_charge_back_fees"]
            transaction["TotalAmazonFees"] = -fees["total"]
            
            # Calculate government VAT (% of item price, negative) - MANUAL from .env
            vat_amount = charges["item_price"] * GOVT_VAT_RATE
            transaction["VAT"] = -vat_amount
            
            # R.VAT - VAT on fees (extracted from API, can be claimed back)
            # This is the Tax component from Commission + FBAPerUnitFulfillmentFee
            fees_vat = fees["referral_fee_tax"] + fees["fba_fee_tax"]
            transaction["R.VAT"] = fees_vat

            # Referral Fee % without VAT
            # Use the base referral fee (without VAT) to calculate percentage
            if charges["item_price"] != 0:
                transaction["Fee%"] = (fees["referral_fee_base"] / charges["item_price"]) * 100
            else:
                transaction["Fee%"] = 0

            # Get and parse cost
            cost = parse_cost(details. get("cost"))
            
            # Store cost of goods
            if cost is None:
                transaction["COG"] = "Not Available"
            else:
                transaction["COG"] = -cost
            
            # Calculate net profit
            net_profit = calculate_profit(
                currency_code=transaction["Currency"],
                sales_proceed=charges["sales_proceed"],
                fees_total=fees["total"],
                vat_amount=vat_amount,
                fees_vat=fees_vat,
                cost=cost
            )
            
            transaction["Net Profit"] = net_profit
            
            transactions.append(transaction)

    elapsed_time = time.time() - start_time
    
    # Print summary
    print("=" * 60)
    print("📊 TRANSACTIONS SUMMARY")
    print("=" * 60)
    print(f"Total API Transactions: {len(api_transactions)}")
    print(f"Total Processed Transactions: {len(transactions)}")
    print(f"Items skipped (no SKU): {skipped['no_sku']}")
    print(f"Items skipped (no mapping): {skipped['no_mapping']}")
    print(f"⏱️  Total processing time: {elapsed_time:.2f} seconds ({elapsed_time / 60:.1f} minutes)")
    print("=" * 60)
    
    return transactions