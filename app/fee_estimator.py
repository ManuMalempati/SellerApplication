#!/usr/bin/env python3
import time
from urllib.parse import quote

import config
from app.auth import spapi_request
from app.rate_limiter import TokenBucketRateLimiter
from tenacity import retry, retry_if_result, stop_after_attempt, wait_exponential

# ============================================================
# Rate limiter (0.5 RPS, burst=1)
# ============================================================

fees_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=1)


# ============================================================
# Tenacity throttling retry
# ============================================================

def _should_retry(result):
    """
    Retry ONLY when Amazon returns throttling errors.
    """
    if not isinstance(result, dict):
        return False

    errors = result.get("errors")
    if not errors:
        return False

    retryable = {"QuotaExceeded", "RequestThrottled"}
    return any(e.get("code") in retryable for e in errors)


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5),
)
def retry_call(func, *args, **kwargs):
    """
    Execute a function with Tenacity retry logic applied.
    Retries only when _should_retry(result) returns True.
    """
    return func(*args, **kwargs)


# ============================================================
# Helpers
# ============================================================

def _safe_float(v):
    try:
        return float(v)
    except Exception:
        return 0.0


def _extract_fee_details(fees_estimate_result):
    """
    Extract referral + FBA fees from the FeesEstimateResult object.
    Returns (referral, fba, debug_dict).
    """
    ref = 0.0
    fba = 0.0
    debug = {}

    fees_estimate = fees_estimate_result.get("FeesEstimate", {}) or {}
    fee_list = fees_estimate.get("FeeDetailList", []) or []

    debug["fee_list_raw"] = fee_list

    for d in fee_list:
        fee_type = (d.get("FeeType") or "").lower()

        amt = _safe_float(
            (d.get("FinalFee") or {}).get("Amount")
            or (d.get("FeeAmount") or {}).get("Amount")
        )

        if "referral" in fee_type:
            ref += amt

        elif "fba" in fee_type or "fbafees" in fee_type:
            fba += amt

            included = d.get("IncludedFeeDetailList", []) or []
            for sub in included:
                sub_amt = _safe_float(
                    (sub.get("FinalFee") or {}).get("Amount")
                    or (sub.get("FeeAmount") or {}).get("Amount")
                )
                fba += sub_amt

    debug["referral"] = ref
    debug["fba"] = fba

    return ref, fba, debug


# ============================================================
# Internal SP-API call (wrapped in Tenacity + rate limit)
# ============================================================

def _call_single_fee_api(id_value, id_type, price):
    """
    Call the SP-API single-item fees endpoint for a given ID.
    Wrapped in:
      - TokenBucketRateLimiter (0.5 RPS)
      - Tenacity retry_call for throttling errors
    """
    body = {{
        "FeesEstimateRequest": {
            "MarketplaceId": config.MARKETPLACE_ID,
            "IsAmazonFulfilled": True,
            "Identifier": f"{id_value}-request",
            "IdType": id_type,
            "IdValue": id_value,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": config.BASE_CURRENCY_CODE,
                    "Amount": float(price),
                }
            },
        }
    }}

    if id_type == "SellerSKU":
        path = f"/products/fees/v0/listings/{quote(id_value)}/feesEstimate"
    else:
        path = f"/products/fees/v0/items/{id_value}/feesEstimate"

    # Global rate limit
    fees_rate_limiter.acquire()

    return retry_call(
        lambda: spapi_request(
            method="POST",
            path=path,
            body=body,
        )
    )


# ============================================================
# Public API
# ============================================================

def get_my_fee_estimate_single(sku, asin, price):
    """
    Final version:
      - Skip invalid price (None, 0, '', '0')
      - SKU → ASIN fallback
      - Manual retry for:
          * missing FeesEstimateResult
          * Status != Success
          * NULL fees (ref=0 and fba=0)
      - Tenacity retry for throttling errors
      - Cleaner logging (warn/error only)
    """

    # --------------------------------------------------------
    # Skip fee estimation if price is invalid
    # --------------------------------------------------------
    if price in (None, 0, "", "0"):
        print(f"[FEE] Skipping fee estimation for {sku} — invalid price={price}")
        return {
            sku or asin: {
                "referral": 0.0,
                "fba": 0.0,
                "debug": {"skipped": True},
            }
        }

    attempts = 3
    delay = 2

    # Try SKU first, then ASIN
    id_attempts = [
        ("SellerSKU", sku),
        ("ASIN", asin),
    ]

    seller_sku_failed = False

    for id_type, id_value in id_attempts:
        if not id_value:
            continue

        for attempt in range(attempts):
            resp = _call_single_fee_api(id_value, id_type, price)

            fees_result = (
                resp.get("payload", {})
                .get("FeesEstimateResult", {})
            )

            # Missing result
            if not fees_result:
                if attempt == attempts - 1:
                    print(f"[FEE][WARN] {id_type}={id_value} → Missing FeesEstimateResult after retries")
                time.sleep(delay)
                delay *= 2
                continue

            # Status not success
            status = fees_result.get("Status")
            if status and status != "Success":
                if attempt == attempts - 1:
                    print(f"[FEE][WARN] {id_type}={id_value} → Status={status} after retries")
                time.sleep(delay)
                delay *= 2
                continue

            # Extract fees
            ref, fba, debug = _extract_fee_details(fees_result)

            # NULL-fee retry
            if ref == 0 and fba == 0:
                if attempt == attempts - 1:
                    print(f"[FEE][WARN] {id_type}={id_value} → NULL fees after retries")
                time.sleep(delay)
                delay *= 2
                continue

            # SUCCESS
            if id_type == "ASIN" and seller_sku_failed:
                print(f"[FEE][DEBUG] Fallback success → SKU={sku} ASIN={asin}")

            return {
                id_value: {
                    "referral": ref,
                    "fba": fba,
                    "debug": debug,
                }
            }

        # Mark SellerSKU failure
        if id_type == "SellerSKU":
            seller_sku_failed = True
            print(f"[FEE][ERROR] FAILED for SellerSKU={id_value}, trying ASIN...")

    # Final failure
    print(f"[FEE][ERROR] FINAL FAIL for {sku or asin} → returning zeros")

    return {
        sku or asin: {
            "referral": 0.0,
            "fba": 0.0,
            "debug": {"error": "NULL after retries"},
        }
    }
