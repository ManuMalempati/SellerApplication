# RESPONSIBLE FOR FinancialTransactions Table
from app.auth import spapi_request
from app.database import get_product_mapping
from app.rate_limiter import TokenBucketRateLimiter
from app.utils import retry_call, to_utc_plus_offset_naive


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
                "Total": item_total,   # ⭐ ITEM-LEVEL TOTAL
            }

            rows.append(row)

    return rows