#!/usr/bin/env python3
from app.utils import retry_call
import config
from app.auth import spapi_request
from app.rate_limiter import TokenBucketRateLimiter

# ============================================================
# Debug toggle
# ============================================================

DEBUG = False   # Set True for verbose logs

# ============================================================
# Rate limiter (0.5 RPS, burst=1)
# ============================================================

fees_rate_limiter = TokenBucketRateLimiter(rate=0.4, burst=1)

# ============================================================
# Helpers
# ============================================================

def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _extract_fee_details(entry):
    """
    Extract referral + FBA fees from a batch FeesEstimate entry.
    Returns (referral, fba).
    """
    ref = 0.0
    fba = 0.0

    fee_list = (
        entry
        .get("FeesEstimate", {})
        .get("FeeDetailList", [])
    )

    for d in fee_list:
        fee_type = (d.get("FeeType") or "").lower()

        amt = _safe_float(
            (d.get("FinalFee") or {}).get("Amount")
            or (d.get("FeeAmount") or {}).get("Amount")
        )

        if "referral" in fee_type:
            ref += amt

        elif "fba" in fee_type or "fulfillment" in fee_type:
            fba += amt

    return ref, fba


# ============================================================
# Batch SP-API call (up to 20 items)
# ============================================================

def _call_batch_fee_api(requests):
    fees_rate_limiter.acquire()
    return retry_call(lambda: spapi_request(
        method="POST",
        path="/products/fees/v0/feesEstimate",
        body=requests
    ))


# ============================================================
# Public API — Batch Version with SKU → ASIN fallback
# ============================================================

def get_my_fee_estimate_batch(items):
    """
    Returns:
        {
            (sku, asin, price): {
                "referral": float | None,
                "fba": float | None,
                "debug": {...}
            }
        }
    """

    # ============================================================
    # 1. Build SKU batch
    # ============================================================

    sku_requests = []
    sku_index_map = []

    for i, item in enumerate(items, start=1):
        sku = item["sku"]
        asin = item["asin"]
        price = item["price"]

        if price in (None, 0, "", "0"):
            if DEBUG:
                print(f"[FEES][SKIP] Invalid price for SKU={sku}, ASIN={asin}, price={price}")
            continue

        sku_requests.append({
            "IdType": "SellerSKU",
            "IdValue": sku,
            "FeesEstimateRequest": {
                "MarketplaceId": config.MARKETPLACE_ID,
                "Identifier": str(i),
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {
                        "Amount": float(price),
                        "CurrencyCode": config.BASE_CURRENCY_CODE
                    }
                }
            }
        })

        sku_index_map.append((sku, asin, price, str(i)))

    results = {}
    failed_for_asin = []

    # ============================================================
    # 2. Execute SKU batch
    # ============================================================

    BATCH_SIZE = 15

    for i in range(0, len(sku_requests), BATCH_SIZE):
        chunk = sku_requests[i:i+BATCH_SIZE]
        chunk_map = sku_index_map[i:i+BATCH_SIZE]

        resp = _call_batch_fee_api(chunk)

        if not isinstance(resp, list):
            if DEBUG:
                print(f"[FEES][ERROR][SKU] Response not list. Resp={resp}")
            for (s, a, p, _) in chunk_map:
                failed_for_asin.append((s, a, p))
            continue

        for entry in resp:
            status = entry.get("Status")
            ident = entry.get("FeesEstimateIdentifier", {}) or {}
            ident_id = ident.get("SellerInputIdentifier")

            sku = asin = price = None
            for (s, a, p, ident_key) in chunk_map:
                if ident_key == ident_id:
                    sku, asin, price = s, a, p
                    break

            if sku is None:
                if DEBUG:
                    print(f"[FEES][WARN][SKU] Could not map entry. Entry={entry}")
                continue

            key = (sku, asin, price)

            # Missing FeesEstimate → missing fees → return None
            if not entry or not entry.get("FeesEstimate"):
                if DEBUG:
                    print(f"[FEES][WARN][SKU] Missing FeesEstimate for {key}, status={status}")
                failed_for_asin.append((sku, asin, price))
                continue

            if status != "Success":
                if DEBUG:
                    print(f"[FEES][WARN][SKU] Non-success status for {key}: {status}")
                failed_for_asin.append((sku, asin, price))
                continue

            ref, fba = _extract_fee_details(entry)

            # Real zero fees → treat as valid
            if ref == 0 and fba == 0:
                if DEBUG:
                    print(f"[FEES][INFO][SKU] Real zero fees for {key}")
                results[key] = {
                    "referral": 0.0,
                    "fba": 0.0,
                    "debug": {"raw": entry, "note": "real_zero_fees"}
                }
                continue

            # Success
            results[key] = {
                "referral": ref,
                "fba": fba,
                "debug": {"raw": entry}
            }

    # ============================================================
    # 3. ASIN fallback batch
    # ============================================================

    asin_requests = []
    asin_index_map = []
    debug_fail_asin = []

    for i, (sku, asin, price) in enumerate(failed_for_asin, start=1):

        if not asin:
            if DEBUG:
                print(f"[FEES][WARN][ASIN] No ASIN for fallback, SKU={sku}")
            results[(sku, asin, price)] = {
                "referral": None,
                "fba": None,
                "debug": {"fallback": "no asin"}
            }
            debug_fail_asin.append((sku, asin, price))
            continue

        asin_requests.append({
            "IdType": "ASIN",
            "IdValue": asin,
            "FeesEstimateRequest": {
                "MarketplaceId": config.MARKETPLACE_ID,
                "Identifier": str(i),
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {
                        "Amount": float(price),
                        "CurrencyCode": config.BASE_CURRENCY_CODE
                    }
                }
            }
        })

        asin_index_map.append((sku, asin, price, str(i)))

    # Execute fallback
    for i in range(0, len(asin_requests), BATCH_SIZE):
        chunk = asin_requests[i:i+BATCH_SIZE]
        chunk_map = asin_index_map[i:i+BATCH_SIZE]

        resp = _call_batch_fee_api(chunk)

        if not isinstance(resp, list):
            if DEBUG:
                print(f"[FEES][ERROR][ASIN] Response not list. Resp={resp}")
            for (s, a, p, _) in chunk_map:
                key = (s, a, p)
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin_resp_not_list"}
                }
                debug_fail_asin.append(key)
            continue

        for entry in resp:
            status = entry.get("Status")
            ident = entry.get("FeesEstimateIdentifier", {}) or {}
            ident_id = ident.get("SellerInputIdentifier")

            sku = asin = price = None
            for (s, a, p, ident_key) in chunk_map:
                if ident_key == ident_id:
                    sku, asin, price = s, a, p
                    break

            if sku is None:
                if DEBUG:
                    print(f"[FEES][WARN][ASIN] Could not map entry. Entry={entry}")
                continue

            key = (sku, asin, price)

            # Missing FeesEstimate → missing fees → return None
            if not entry or not entry.get("FeesEstimate"):
                if DEBUG:
                    print(f"[FEES][WARN][ASIN] Missing FeesEstimate for {key}, status={status}")
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin missing"}
                }
                debug_fail_asin.append(key)
                continue

            if status != "Success":
                if DEBUG:
                    print(f"[FEES][WARN][ASIN] Non-success status for {key}: {status}")
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin failed", "status": status}
                }
                debug_fail_asin.append(key)
                continue

            ref, fba = _extract_fee_details(entry)

            # Real zero fees → valid
            if ref == 0 and fba == 0:
                if DEBUG:
                    print(f"[FEES][INFO][ASIN] Real zero fees for {key}")
                results[key] = {
                    "referral": 0.0,
                    "fba": 0.0,
                    "debug": {"fallback": "asin_zero_fees", "raw": entry}
                }
                continue

            # Success
            results[key] = {
                "referral": ref,
                "fba": fba,
                "debug": {"fallback": "asin", "raw": entry}
            }

    # ============================================================
    # Summary (always printed)
    # ============================================================

    print(f"[FEES][SUMMARY] SKU failures needing ASIN fallback: {len(failed_for_asin)}")
    print(f"[FEES][SUMMARY] ASIN final failures (still missing): {len(debug_fail_asin)}")

    return results