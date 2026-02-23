import os
import time
import threading
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

from .auth import spapi_request
from .database import get_product_mapping


# ---------------------------------------------------------
# Rate Limiter
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

                if self.tokens >= 1:
                    self.tokens -= 1
                    return

                wait_time = (1 - self.tokens) / self.rate

            time.sleep(wait_time)


transactions_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=10)


# ---------------------------------------------------------
# Retry Logic
# ---------------------------------------------------------
def _should_retry(result):
    if isinstance(result, dict) and "errors" in result:
        codes = [e.get("code") for e in result["errors"]]
        return "QuotaExceeded" in codes or "RequestThrottled" in codes
    return False


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5),
)
def retry_call(func, *args, **kwargs):
    return func(*args, **kwargs)


# ---------------------------------------------------------
# Safe decimal
# ---------------------------------------------------------
def safe(value):
    try:
        return float(value)
    except:
        return 0.0


# ---------------------------------------------------------
# Retrieve Finances 2024-06-19 API transactions
# ---------------------------------------------------------
def retrieve_transactions(params):
    all_tx = []

    def _fetch_initial():
        transactions_rate_limiter.acquire()
        return spapi_request(
            method="GET",
            path="/finances/2024-06-19/transactions",
            params=params
        )

    response = retry_call(_fetch_initial)
    payload = response.get("payload", {})
    tx_list = payload.get("transactions", [])
    all_tx.extend(tx_list)

    next_token = payload.get("nextToken")

    while next_token:
        def _fetch_page():
            transactions_rate_limiter.acquire()
            return spapi_request(
                method="GET",
                path="/finances/2024-06-19/transactions",
                params={"nextToken": next_token}
            )

        response = retry_call(_fetch_page)
        payload = response.get("payload", {})
        tx_list = payload.get("transactions", [])
        all_tx.extend(tx_list)

        next_token = payload.get("nextToken")

    return all_tx


# ---------------------------------------------------------
# Extract breakdown values
# ---------------------------------------------------------
def extract_breakdown(breakdowns, target):
    if not breakdowns:
        return 0.0

    total = 0.0

    for b in breakdowns:
        if b.get("breakdownType") == target:
            amt = safe(b.get("breakdownAmount", {}).get("currencyAmount"))
            total += amt

        nested = b.get("breakdowns")
        if nested:
            total += extract_breakdown(nested, target)

    return total


# ---------------------------------------------------------
# Main Processor
# ---------------------------------------------------------
def get_transactions(params, db_cursor):
    print("Fetching Finances 2024-06-19 Transactions...")

    txs = retrieve_transactions(params)
    if not txs:
        print("No transactions found")
        return []

    # Extract SKUs for mapping
    all_skus = []
    for tx in txs:
        for item in tx.get("items", []):
            for ctx in item.get("contexts", []):
                if ctx.get("contextType") == "ProductContext":
                    sku = ctx.get("sku")
                    if sku:
                        all_skus.append(sku)

    all_skus = list(set(all_skus))
    product_mapping = get_product_mapping(db_cursor, all_skus)

    rows = []

    for tx in txs:
        transaction_id = tx.get("transactionId")
        posted_date = tx.get("postedDate")
        transaction_type = tx.get("transactionType")
        transaction_status = tx.get("transactionStatus")

        amazon_order_id = None
        for rid in tx.get("relatedIdentifiers", []):
            if rid.get("relatedIdentifierName") == "ORDER_ID":
                amazon_order_id = rid.get("relatedIdentifierValue")

        for item in tx.get("items", []):
            # Extract SKU, ASIN, qty
            sku = None
            asin = None
            qty = None

            for ctx in item.get("contexts", []):
                if ctx.get("contextType") == "ProductContext":
                    sku = ctx.get("sku")
                    asin = ctx.get("asin")
                    qty = ctx.get("quantityShipped")

            if not sku:
                continue

            mapping = product_mapping.get(sku, {})
            ssku = mapping.get("ssku")

            # Extract breakdowns
            breakdowns = item.get("breakdowns", [])

            principal = extract_breakdown(breakdowns, "OurPricePrincipal")
            shipping = extract_breakdown(breakdowns, "ShippingPrincipal")
            promotions = extract_breakdown(breakdowns, "PromoRebates")

            fba_fees = extract_breakdown(breakdowns, "FBAPerUnitFulfillmentFee")
            commission = extract_breakdown(breakdowns, "Commission")
            shipping_chargeback = extract_breakdown(breakdowns, "ShippingChargeback")

            fixed_closing = 0.0
            variable_closing = 0.0

            ref_fee = commission + fixed_closing + variable_closing

            total = (
                principal
                + shipping
                + promotions
                + fba_fees
                + commission
                + shipping_chargeback
            )

            row = {
                "TransactionId": transaction_id,
                "PostedDate": posted_date,
                "TransactionStatus": transaction_status,
                "TransactionType": transaction_type,
                "AmazonOrderId": amazon_order_id,

                "SellerSKU": sku,
                "ASIN": asin,
                "SSKU": ssku,
                "QuantityShipped": qty,

                "Principal": principal,
                "ShippingCharges": shipping,
                "Promotions": promotions,

                "FBAFees": fba_fees,
                "Commission": commission,
                "FixedClosingFee": fixed_closing,
                "VariableClosingFee": variable_closing,
                "ShippingChargeback": shipping_chargeback,
                "RefFee": ref_fee,

                "Total": total,
            }

            rows.append(row)

    return rows
