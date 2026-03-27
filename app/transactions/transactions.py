"""
FinancialTransactions Ingestion Pipeline
========================================

This module retrieves Amazon Finances API (2024‑06‑19) transaction data and
normalizes it into rows suitable for insertion into the `FinancialTransactions`
table. It handles all transaction types returned by the Finances API, including
orders, refunds, fees, and FBA inventory reimbursements.

Pipeline Responsibilities
-------------------------

1. Retrieve Transactions (Paginated)
   - Calls `/finances/2024-06-19/transactions` with rate‑limiting and retry logic.
   - Follows `nextToken` pagination until all pages are retrieved.
   - Produces a complete list of raw transaction objects.

2. Product Mapping
   - Extracts all SKUs referenced inside transaction item contexts.
   - Loads SKU → SSKU mappings for downstream normalization.

3. Special Case: FBAInventoryReimbursement
   - Amazon provides only transaction‑level totals (no item‑level breakdowns).
   - For these transactions:
        • SKU/ASIN/qty extracted from the first item  
        • Total = transaction‑level `totalAmount`  
        • All fee fields set to zero  
   - A single row is emitted per reimbursement transaction.

4. Normal Item‑Level Processing
   For all other transaction types (orders, refunds, adjustments):
   - Each item in the transaction becomes one row.
   - Extracts SKU, ASIN, quantity, and item‑level totals.
   - Recursively extracts financial components from `breakdowns`:
        • Principal (OurPricePrincipal)  
        • Shipping charges  
        • Promotions  
        • FBA fees  
        • Commission / RefundCommission  
        • Shipping chargebacks  
   - Computes:
        • RefFee = commission + closing fees  
        • Total = item‑level `totalAmount` (Amazon’s authoritative value)

5. Output
   - Returns a fully normalized list of dictionaries, each representing a single
     financial event at the SKU‑item level.
   - The caller is responsible for inserting/upserting into the
     `FinancialTransactions` table.

"""

from app.auth import spapi_request
from app.database import get_product_mapping
from app.utilities.rate_limiter import TokenBucketRateLimiter
from app.utilities.utils import retry_call, to_utc_plus_offset_naive


# ---------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------

transactions_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=10)

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
    tx_list = payload.get("transactions") or []
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
        tx_list = payload.get("transactions") or []
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
        items = tx.get("items") or []
        for item in items:
            contexts = item.get("contexts") or []
            for ctx in contexts:
                if ctx.get("contextType") == "ProductContext":
                    sku = ctx.get("sku")
                    if sku:
                        all_skus.append(sku)

    all_skus = list(set(all_skus))
    product_mapping = get_product_mapping(db_cursor, all_skus)

    rows = []

    for tx in txs:
        posted_date = to_utc_plus_offset_naive(tx.get("postedDate"))
        transaction_type = tx.get("transactionType")
        transaction_status = tx.get("transactionStatus")

        amazon_order_id = None
        for rid in tx.get("relatedIdentifiers") or []:
            if rid.get("relatedIdentifierName") == "ORDER_ID":
                amazon_order_id = rid.get("relatedIdentifierValue")

        # ---------------------------------------------------------
        # SPECIAL CASE: FBAInventoryReimbursement
        # ---------------------------------------------------------
        if transaction_type == "FBAInventoryReimbursement":
            sku = asin = qty = None

            items = tx.get("items") or []
            if items:
                item = items[0]
                for ctx in item.get("contexts") or []:
                    if ctx.get("contextType") == "ProductContext":
                        sku = ctx.get("sku")
                        asin = ctx.get("asin")
                        qty = ctx.get("quantityShipped")

            mapping = product_mapping.get(sku, {}) if sku else {}
            ssku = mapping.get("ssku")

            # For reimbursements, Amazon gives only transaction-level totals.
            # We keep using that because item-level totals do not exist.
            api_total = safe(tx.get("totalAmount", {}).get("currencyAmount"))

            row = {
                "PostedDate": posted_date,
                "TransactionStatus": transaction_status,
                "TransactionType": transaction_type,
                "AmazonOrderId": amazon_order_id,

                "SellerSKU": sku,
                "ASIN": asin,
                "SSKU": ssku,
                "QuantityShipped": qty,

                "Principal": api_total,
                "ShippingCharges": 0.0,
                "Promotions": 0.0,

                "FBAFees": 0.0,
                "RefundCommission": 0.0,
                "FixedClosingFee": 0.0,
                "VariableClosingFee": 0.0,
                "ShippingChargeback": 0.0,

                "RefFee": 0.0,
                "Total": api_total,
            }

            rows.append(row)
            continue

        # ---------------------------------------------------------
        # NORMAL ORDER / REFUND / OTHER ITEM-BASED PROCESSING
        # ---------------------------------------------------------
        for item in tx.get("items") or []:
            sku = asin = qty = None

            for ctx in item.get("contexts") or []:
                if ctx.get("contextType") == "ProductContext":
                    sku = ctx.get("sku")
                    asin = ctx.get("asin")
                    qty = ctx.get("quantityShipped")

            if not sku:
                continue

            mapping = product_mapping.get(sku, {})
            ssku = mapping.get("ssku")

            breakdowns = item.get("breakdowns") or []

            principal = extract_breakdown(breakdowns, "OurPricePrincipal")
            shipping = extract_breakdown(breakdowns, "ShippingPrincipal")
            promotions = extract_breakdown(breakdowns, "PromoRebates")

            fba_fees = extract_breakdown(breakdowns, "FBAPerUnitFulfillmentFee")
            commission = extract_breakdown(breakdowns, "Commission")
            refund_commission = extract_breakdown(breakdowns, "RefundCommission")
            shipping_chargeback = extract_breakdown(breakdowns, "ShippingChargeback")

            fixed_closing = 0.0
            variable_closing = 0.0

            ref_fee = commission + fixed_closing + variable_closing

            # ⭐ ITEM-LEVEL TOTAL (NEW RULE)
            item_total = safe(item.get("totalAmount", {}).get("currencyAmount"))

            row = {
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
                "RefundCommission": refund_commission,
                "FixedClosingFee": fixed_closing,
                "VariableClosingFee": variable_closing,
                "ShippingChargeback": shipping_chargeback,

                "RefFee": ref_fee,
                "Total": item_total,   # ITEM-LEVEL TOTAL
            }

            rows.append(row)

    return rows