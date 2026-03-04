import time
import random
import config
from app.utilities.utils import retry_call, safe_float   # <-- use ingestion-grade safe_float
from app.auth import spapi_request
from app.utilities.rate_limiter import TokenBucketRateLimiter

DEBUG = False
fees_rate_limiter = TokenBucketRateLimiter(rate=0.49, burst=1)

# =========================================================
# Unified retry decision logic
# =========================================================

def should_retry(resp=None, exc=None):
    """
    Unified retry logic:
    - Return False → do NOT retry (normal or client error)
    - Return True  → retry (non-client error or exception)
    """

    # -------------------------
    # Exception-based handling
    # -------------------------
    if exc is not None:
        resp_obj = getattr(exc, "response", None)

        # If exception contains a 4xx response → client error → do NOT retry
        if resp_obj is not None:
            try:
                if 400 <= resp_obj.status_code < 500:
                    return False
            except:
                pass

        # All other exceptions → retry
        return True

    # -------------------------
    # Response-based handling
    # -------------------------
    if not isinstance(resp, dict):
        return True  # malformed → retry

    # Amazon top-level status
    status = resp.get("Status")
    if status and status != "Success":
        return True  # ServerError, ProcessingError, etc.

    # Error block
    err = resp.get("Error") or resp.get("errors")
    if not err:
        return False  # normal response → do NOT retry

    if isinstance(err, dict):
        code = str(err.get("Code") or err.get("code") or "")
        if code.startswith("4"):
            return False  # client error → do NOT retry
        return True       # non-client error → retry

    return True  # weird structure → retry


# =========================================================
# Extract fee details (correct None handling)
# =========================================================

def _extract_fee_details(entry):
    """
    Extract referral + FBA fees from Amazon's FeeDetailList.
    Missing or invalid fee amounts remain None and are skipped.
    Real zero fees remain 0.0.
    """
    ref = 0.0
    fba = 0.0

    fee_list = entry.get("FeesEstimate", {}).get("FeeDetailList", [])

    for d in fee_list:
        fee_type = (d.get("FeeType") or "").lower()

        amt = safe_float(
            (d.get("FinalFee") or {}).get("Amount")
            or (d.get("FeeAmount") or {}).get("Amount")
        )

        # Skip missing fees
        if amt is None:
            continue

        if "referral" in fee_type:
            ref += amt
        elif "fba" in fee_type or "fulfillment" in fee_type:
            fba += amt

    return ref, fba


# =========================================================
# Call Amazon batch fee API with throttling + unified retry
# =========================================================

def _call_batch_fee_api(requests):
    fees_rate_limiter.acquire()
    last_resp = None

    for attempt in range(3):
        try:
            resp = retry_call(lambda: spapi_request(
                method="POST",
                path="/products/fees/v0/feesEstimate",
                body=requests
            ))
            last_resp = resp

        except Exception as e:
            # Unified retry logic for exceptions
            if not should_retry(exc=e):
                print(f"[FEES][ERROR] Client error exception {e}. Not retrying.")
                return {"Error": {"Code": "4xx", "Message": str(e)}}

            wait = 0.5 + random.random() * 1.5
            print(f"[FEES][WARN] Exception {type(e).__name__}: {e}. Retrying in {wait:.2f}s (attempt {attempt+1}/3)")
            time.sleep(wait)
            continue

        # Debug raw response
        print(f"[FEES][DEBUG] Attempt {attempt+1}/3 response: {resp}")

        # Unified retry logic for responses
        if should_retry(resp=resp):
            wait = 0.5 + random.random() * 1.5
            print(f"[FEES][WARN] Non-client error detected. Retrying in {wait:.2f}s (attempt {attempt+1}/3)")
            time.sleep(wait)
            continue

        # Normal or client error → return immediately
        time.sleep(0.2 + random.random() * 0.3)
        return resp

    print("[FEES][ERROR] Non-client error persisted after retries.")
    return last_resp

# =========================================================
# Main batch fee estimator
# =========================================================

def get_my_fee_estimate_batch(items):

    sku_requests = []
    sku_index_map = []

    # -----------------------------------------------------
    # Build SKU requests
    # -----------------------------------------------------
    for i, item in enumerate(items, start=1):
        sku = item["sku"]
        asin = item["asin"]
        price = item["price"]

        if price in (None, 0, "", "0"):
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
    asin_raw_errors = {}

    BATCH_SIZE = 20
    total_batches = (len(sku_requests) + BATCH_SIZE - 1) // BATCH_SIZE

    # =====================================================
    # SKU batches
    # =====================================================

    for i in range(0, len(sku_requests), BATCH_SIZE):
        batch_index = i // BATCH_SIZE
        progress = (batch_index / total_batches) * 100

        if 25 <= progress < 25.5:
            print("[FEES][PROGRESS] 25% of SKU batches processed")
        elif 50 <= progress < 50.5:
            print("[FEES][PROGRESS] 50% of SKU batches processed")
        elif 75 <= progress < 75.5:
            print("[FEES][PROGRESS] 75% of SKU batches processed")

        chunk = sku_requests[i:i+BATCH_SIZE]
        chunk_map = sku_index_map[i:i+BATCH_SIZE]

        resp = _call_batch_fee_api(chunk)

        if not isinstance(resp, list):
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
                continue

            key = (sku, asin, price)

            if not entry.get("FeesEstimate"):
                failed_for_asin.append((sku, asin, price))
                continue

            if status != "Success":
                failed_for_asin.append((sku, asin, price))
                continue

            ref, fba = _extract_fee_details(entry)

            # Real zero fees
            if ref == 0 and fba == 0:
                results[key] = {"referral": 0.0, "fba": 0.0, "debug": {"note": "real_zero_fees"}}
                continue

            results[key] = {"referral": ref, "fba": fba, "debug": {}}

    # =====================================================
    # ASIN fallback
    # =====================================================

    asin_requests = []
    asin_index_map = []
    debug_fail_asin = []

    for i, (sku, asin, price) in enumerate(failed_for_asin, start=1):

        if not asin:
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

    total_asin_batches = (len(asin_requests) + BATCH_SIZE - 1) // BATCH_SIZE

    for i in range(0, len(asin_requests), BATCH_SIZE):
        asin_batch_index = i // BATCH_SIZE
        asin_progress = (asin_batch_index / total_asin_batches) * 100

        if 25 <= asin_progress < 25.5:
            print("[FEES][PROGRESS] 25% of ASIN fallback batches processed")
        elif 50 <= asin_progress < 50.5:
            print("[FEES][PROGRESS] 50% of ASIN fallback batches processed")
        elif 75 <= asin_progress < 75.5:
            print("[FEES][PROGRESS] 75% of ASIN fallback batches processed")

        chunk = asin_requests[i:i+BATCH_SIZE]
        chunk_map = asin_index_map[i:i+BATCH_SIZE]

        resp = _call_batch_fee_api(chunk)

        if not isinstance(resp, list):
            for (s, a, p, _) in chunk_map:
                key = (s, a, p)
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin_resp_not_list"}
                }
                debug_fail_asin.append(key)
                asin_raw_errors[key] = {"request": chunk_map, "response": resp}
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
                continue

            key = (sku, asin, price)

            if not entry.get("FeesEstimate"):
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin missing"}
                }
                debug_fail_asin.append(key)
                asin_raw_errors[key] = {"request": chunk_map, "response": entry}
                continue

            if status != "Success":
                results[key] = {
                    "referral": None,
                    "fba": None,
                    "debug": {"fallback": "asin failed", "status": status}
                }
                debug_fail_asin.append(key)
                asin_raw_errors[key] = {"request": chunk_map, "response": entry}
                continue

            ref, fba = _extract_fee_details(entry)

            if ref == 0 and fba == 0:
                results[key] = {
                    "referral": 0.0,
                    "fba": 0.0,
                    "debug": {"fallback": "asin_zero_fees"}
                }
                continue

            results[key] = {
                "referral": ref,
                "fba": fba,
                "debug": {"fallback": "asin"}
            }

    # =====================================================
    # FINAL FAILURE DEBUG
    # =====================================================

    print(f"[FEES][SUMMARY] SKU failures needing ASIN fallback: {len(failed_for_asin)}")
    print(f"[FEES][SUMMARY] ASIN final failures (still missing): {len(debug_fail_asin)}")

    if debug_fail_asin:
        print("\n[FEES][DEBUG] FINAL ASIN FAILURES (with request + raw response):")
        for key in debug_fail_asin:
            sku, asin, price = key
            print(f"\n--- FAILURE ---")
            print(f"SKU={sku}, ASIN={asin}, PRICE={price}")

            err = asin_raw_errors.get(key)
            if err:
                print("REQUEST PAYLOAD:")
                print(err["request"])
                print("RAW AMAZON RESPONSE:")
                print(err["response"])

    return results