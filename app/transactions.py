import json
import os
import time
import threading
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost
)
from .auth import spapi_request


BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
GOVT_VAT_RATE = 1 / float(os.getenv("GOVT_VAT_RATE_DIVISOR"))

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
INITIAL_RETRY_DELAY = float(os.getenv("INITIAL_RETRY_DELAY", "5.0"))


# ---------------------------------------------------------
# Token Bucket Rate Limiter
# ---------------------------------------------------------
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


transactions_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=10)


# ---------------------------------------------------------
# Tenacity Retry Logic
# ---------------------------------------------------------
def _should_retry(result):
    if isinstance(result, dict) and "errors" in result:
        error_codes = [err.get("code") for err in result.get("errors", [])]
        return "QuotaExceeded" in error_codes or "RequestThrottled" in error_codes
    return False


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(MAX_RETRIES),
    wait=wait_exponential(multiplier=INITIAL_RETRY_DELAY, min=INITIAL_RETRY_DELAY),
)
def tenacity_retry_call(func, *args, **kwargs):
    result = func(*args, **kwargs)

    if isinstance(result, dict) and "errors" in result:
        errors = result["errors"]
        error_codes = [err.get("code") for err in errors]

        if "QuotaExceeded" in error_codes or "RequestThrottled" in error_codes:
            print("Rate limit hit, retrying")
        else:
            print(f"API Error: {errors}")

    return result


# ---------------------------------------------------------
# Retrieve Transactions (pagination)
# ---------------------------------------------------------
def retrieve_transaction_list(method, path, params):
    all_transactions = []
    page_count = 0

    def _fetch_initial():
        transactions_rate_limiter.acquire()
        return spapi_request(method=method, path=path, params=params)

    json_response = tenacity_retry_call(_fetch_initial)

    if "errors" in json_response:
        print(f"Error on initial request: {json_response.get('errors')}")
        return all_transactions

    payload = json_response.get("payload")
    if not payload:
        return all_transactions

    transactions = payload.get("transactions", [])
    all_transactions.extend(transactions)
    page_count += 1
    print(f"Page {page_count}: Retrieved {len(transactions)} transactions")

    next_token = payload.get("nextToken")

    while next_token:
        def _fetch_page():
            transactions_rate_limiter.acquire()
            return spapi_request(
                method=method,
                path=path,
                params={"nextToken": next_token}
            )

        json_response = tenacity_retry_call(_fetch_page)

        if "errors" in json_response:
            print(f"Error on page {page_count + 1}: {json_response.get('errors')}")
            break

        payload = json_response.get("payload")
        if not payload:
            break

        transactions = payload.get("transactions", [])
        all_transactions.extend(transactions)
        page_count += 1
        print(f"Page {page_count}: Retrieved {len(transactions)} transactions")

        next_token = payload.get("nextToken")

    print(f"Total pages retrieved: {page_count}")
    print(f"Total transactions from API: {len(all_transactions)}")

    return all_transactions


# ---------------------------------------------------------
# Breakdown Extractor
# ---------------------------------------------------------
def extract_breakdown_value(breakdowns, breakdown_type, sub_type=None):
    if not breakdowns:
        return 0

    for breakdown in breakdowns:
        if breakdown.get("breakdownType") == breakdown_type:
            if sub_type:
                nested = breakdown.get("breakdowns", [])
                for nested_breakdown in nested:
                    if nested_breakdown.get("breakdownType") == sub_type:
                        amount = nested_breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                        return abs(amount) if amount else 0
            else:
                amount = breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                return abs(amount) if amount else 0

        nested = breakdown.get("breakdowns")
        if nested:
            result = extract_breakdown_value(nested, breakdown_type, sub_type)
            if result != 0:
                return result

    return 0


# ---------------------------------------------------------
# Fee Calculations
# ---------------------------------------------------------
def calculate_fees_from_transaction(item):
    item_breakdowns = item.get("breakdowns", [])

    referral_fee_base = extract_breakdown_value(item_breakdowns, "Commission", "Base")
    referral_fee_tax = extract_breakdown_value(item_breakdowns, "Commission", "Tax")
    referral_fee_total = referral_fee_base + referral_fee_tax

    fba_fee_base = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Base")
    fba_fee_tax = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Tax")
    fba_fee_total = fba_fee_base + fba_fee_tax

    shipping_charge_back_fees = extract_breakdown_value(item_breakdowns, "ShippingChargeback")

    total_fees = referral_fee_total + fba_fee_total + shipping_charge_back_fees

    return {
        "referral_fee_base": referral_fee_base,
        "referral_fee_tax": referral_fee_tax,
        "referral_fee_total": referral_fee_total,
        "fba_fee_base": fba_fee_base,
        "fba_fee_tax": fba_fee_tax,
        "fba_fee_total": fba_fee_total,
        "shipping_charge_back_fees": shipping_charge_back_fees,
        "total": total_fees
    }


# ---------------------------------------------------------
# Customer Charges
# ---------------------------------------------------------
def calculate_customer_charges_from_transaction(item):
    item_breakdowns = item.get("breakdowns", [])

    item_price = extract_breakdown_value(item_breakdowns, "OurPricePrincipal")

    shipping_charge = extract_breakdown_value(item_breakdowns, "Shipping")
    if shipping_charge == 0:
        shipping_charge = extract_breakdown_value(item_breakdowns, "ShippingPrincipal")

    total_promotions = extract_breakdown_value(item_breakdowns, "PromoRebates")
    if total_promotions == 0:
        total_promotions = extract_breakdown_value(item_breakdowns, "ShippingDiscount")

    total_charges = item_price + shipping_charge
    sales_proceed = total_charges - total_promotions

    return {
        "item_price": item_price,
        "shipping_charge": shipping_charge,
        "total_charges": total_charges,
        "total_promotions": total_promotions,
        "sales_proceed": sales_proceed
    }


# ---------------------------------------------------------
# Profit Calculation
# ---------------------------------------------------------
def calculate_profit(currency_code, sales_proceed, fees_total, vat_amount, fees_vat, cost):
    if cost is None:
        return "Not Available"
    elif currency_code != BASE_CURRENCY_CODE:
        return "Currency Code Different"

    net_profit = sales_proceed - fees_total - vat_amount + fees_vat - cost
    return net_profit


# ---------------------------------------------------------
# Main Transaction Processor
# ---------------------------------------------------------
def get_transactions(params, db_cursor):
    start_time = time.time()

    print("============================================================")
    print("FETCHING FINANCIAL TRANSACTIONS")
    print("============================================================")

    method = "GET"
    path = "/finances/2024-06-19/transactions"

    print("Retrieving transactions from Amazon SP-API...")
    api_transactions = retrieve_transaction_list(method, path, params)

    if not api_transactions:
        print("============================================================")
        print("TRANSACTIONS SUMMARY")
        print("============================================================")
        print("Total API Transactions: 0")
        print("Total Processed Transactions: 0")
        print("============================================================")
        return []

    print("Extracting SKUs from transactions...")
    all_seller_skus = []
    for transaction in api_transactions:
        for item in transaction.get("items", []):
            contexts = item.get("contexts") or []
            for context in contexts:
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    if sku:
                        all_seller_skus.append(sku)

    all_seller_skus = list(set(all_seller_skus))
    print(f"Found {len(all_seller_skus)} unique SKUs")

    print("Fetching product mappings from database...")
    product_mapping = get_product_mapping(db_cursor, all_seller_skus)
    print(f"Retrieved mappings for {len(product_mapping)} SKUs")

    all_asins = list(set([
        mapping["asin"]
        for mapping in product_mapping.values()
        if "asin" in mapping
    ]))

    print(f"Fetching product details for {len(all_asins)} ASINs...")
    asin_details = get_product_details_by_asin(db_cursor, all_asins)
    print(f"Retrieved details for {len(asin_details)} ASINs")

    print("Processing transactions...")
    transactions = []
    skipped = {"no_sku": 0, "no_mapping": 0}

    for api_transaction in api_transactions:
        transaction_type = api_transaction.get("transactionType", "Unknown")
        transaction_status = api_transaction.get("transactionStatus", "Unknown")
        transaction_id = api_transaction.get("transactionId")
        posted_date = api_transaction.get("postedDate")

        order_id = None
        for identifier in api_transaction.get("relatedIdentifiers", []):
            if identifier.get("relatedIdentifierName") == "ORDER_ID":
                order_id = identifier.get("relatedIdentifierValue")
                break

        for item in api_transaction.get("items", []):
            sku = None
            asin = None

            contexts = item.get("contexts") or []
            for context in contexts:
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    asin = context.get("asin")
                    break

            if not sku:
                skipped["no_sku"] += 1
                continue

            transaction = {}
            transaction["TransactionId"] = transaction_id
            transaction["PostedDate"] = posted_date
            transaction["TransactionType"] = transaction_type
            transaction["TransactionStatus"] = transaction_status
            transaction["AmazonOrderId"] = order_id or "Not Available"
            transaction["SKU"] = sku

            mapping = product_mapping.get(sku, {})
            if not mapping:
                skipped["no_mapping"] += 1

            transaction["ASIN"] = asin or mapping.get("asin", "Not Available")
            transaction["SSKU"] = mapping.get("ssku", "Not Available")

            asin_key = transaction["ASIN"]
            details = asin_details.get(asin_key, {})

            transaction["Brand"] = details.get("brand", "Not Available")
            transaction["Category"] = details.get("category", "Not Available")

            charges = calculate_customer_charges_from_transaction(item)

            transaction["Currency"] = item.get("totalAmount", {}).get("currencyCode", BASE_CURRENCY_CODE)

            transaction["SOLD"] = charges["item_price"]
            transaction["ShippingCharge"] = charges["shipping_charge"]
            transaction["TotalPromotions"] = -charges["total_promotions"]
            transaction["SalesProceed"] = charges["sales_proceed"]

            fees = calculate_fees_from_transaction(item)

            transaction["Fee"] = -fees["referral_fee_total"]
            transaction["FBAFees"] = -fees["fba_fee_total"]
            transaction["ShippingChargeback"] = -fees["shipping_charge_back_fees"]
            transaction["TotalAmazonFees"] = -fees["total"]

            vat_amount = charges["item_price"] * GOVT_VAT_RATE
            transaction["VAT"] = -vat_amount

            fees_vat = fees["referral_fee_tax"] + fees["fba_fee_tax"]
            transaction["R.VAT"] = fees_vat

            if charges["item_price"] != 0:
                transaction["Fee%"] = (fees["referral_fee_base"] / charges["item_price"]) * 100
            else:
                transaction["Fee%"] = 0

            cost = parse_cost(details.get("cost"))

            if cost is None:
                transaction["COG"] = "Not Available"
            else:
                transaction["COG"] = -cost

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

    print("============================================================")
    print("TRANSACTIONS SUMMARY")
    print("============================================================")
    print(f"Total API Transactions: {len(api_transactions)}")
    print(f"Total Processed Transactions: {len(transactions)}")
    print(f"Items skipped (no SKU): {skipped['no_sku']}")
    print(f"Items skipped (no mapping): {skipped['no_mapping']}")
    print(f"Total processing time: {elapsed_time:.2f} seconds ({elapsed_time / 60:.1f} minutes)")
    print("============================================================")

    return transactions