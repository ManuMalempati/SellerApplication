#!/usr/bin/env python3
import asyncio
import csv
import time
from io import StringIO
from urllib.parse import quote
import os

from .. import config
config.load_env()

from app.database import connect_database, parse_cost, get_product_details_by_asin
from app.fba.helpers import request_report, wait_for_report, download_report
from config import GOVT_VAT_RATE
from app.auth import spapi_request

# Standardized helper from utils
from app.utils import get_now_iso_string_with_custom_utc_offset

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")
FEE_RETRY_ATTEMPTS = 2


# ---------------------------------------------------------
# RATE LIMITER (burst=2, rate=1 RPS)
# ---------------------------------------------------------
_last_call_time = 0
_burst_tokens = 2

def rate_limit():
    global _last_call_time, _burst_tokens

    now = time.time()

    if now - _last_call_time >= 1:
        _burst_tokens = min(2, _burst_tokens + 1)
        _last_call_time = now

    if _burst_tokens == 0:
        sleep_time = 1 - (now - _last_call_time)
        if sleep_time > 0:
            time.sleep(sleep_time)
        _burst_tokens = 1
        _last_call_time = time.time()
    else:
        _burst_tokens -= 1


# ---------------------------------------------------------
# Fee Estimation Logic (SELF-CONTAINED)
# ---------------------------------------------------------
def _safe_float(value):
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _extract_from_response(response):
    if not isinstance(response, dict):
        return None

    if "errors" in response:
        return {"errors": response.get("errors")}

    result_root = (
        (response.get("payload") or {}).get("FeesEstimateResult")
        or response.get("FeesEstimateResult")
        or {}
    )

    status_value = result_root.get("Status")
    if status_value and status_value != "Success":
        return None

    fees_section = (
        result_root.get("FeesEstimate")
        or response.get("FeesEstimate")
        or {}
    )

    total_section = fees_section.get("TotalFeesEstimate") or {}
    total_fees_amount = _safe_float(total_section.get("Amount"))

    referral_fee_net = 0.0
    fba_fee_net = 0.0

    fee_detail_list = fees_section.get("FeeDetailList") or []

    if fee_detail_list:
        for fee_item in fee_detail_list:
            fee_type = (fee_item.get("FeeType") or "").lower()
            final_fee_amount = _safe_float(
                (fee_item.get("FinalFee") or {}).get("Amount")
                or (fee_item.get("FeeAmount") or {}).get("Amount")
            )
            if final_fee_amount is None:
                continue

            if "referral" in fee_type:
                referral_fee_net += final_fee_amount
            elif "fba" in fee_type or "pick" in fee_type:
                fba_fee_net += final_fee_amount
    else:
        referral_fee_net = _safe_float(
            fees_section.get("ReferralFee")
            or fees_section.get("ReferralFees")
            or 0.0
        ) or 0.0

        fba_fee_net = _safe_float(
            fees_section.get("FBAFees")
            or fees_section.get("FBAFee")
            or 0.0
        ) or 0.0

    return {
        "raw": response,
        "net": {
            "TotalAmazonFees": total_fees_amount,
            "ReferralFees": referral_fee_net,
            "FBAFees": fba_fee_net,
        },
    }


def _request_fees(sku, asin, price):
    rate_limit()

    # SKU-based request
    if sku:
        try:
            safe_sku = quote(sku, safe="")
            body = {
                "FeesEstimateRequest": {
                    "MarketplaceId": MARKETPLACE_ID or "",
                    "IsAmazonFulfilled": True,
                    "PriceToEstimateFees": {
                        "ListingPrice": {
                            "CurrencyCode": BASE_CURRENCY_CODE,
                            "Amount": price,
                        }
                    },
                    "Identifier": f"{sku}-estimate",
                }
            }

            resp = spapi_request(
                "POST",
                f"/products/fees/v0/listings/{safe_sku}/feesEstimate",
                body=body,
            )

            extracted = _extract_from_response(resp)
            if extracted is not None:
                return extracted

        except Exception:
            pass

    # ASIN-based request
    if asin:
        try:
            body = {
                "FeesEstimateRequest": {
                    "MarketplaceId": MARKETPLACE_ID or "",
                    "IsAmazonFulfilled": True,
                    "PriceToEstimateFees": {
                        "ListingPrice": {
                            "CurrencyCode": BASE_CURRENCY_CODE,
                            "Amount": price,
                        }
                    },
                    "Identifier": f"{asin}-estimate",
                }
            }

            resp = spapi_request(
                "POST",
                f"/products/fees/v0/items/{asin}/feesEstimate",
                body=body,
            )

            extracted = _extract_from_response(resp)
            if extracted is not None:
                return extracted

        except Exception:
            pass

    return None


def get_fees_estimate_local(sku, asin, price):
    if price is None:
        return {"errors": [{"code": "InvalidPrice", "message": "Price is required"}]}

    attempt = 0
    last_result = None

    while attempt < FEE_RETRY_ATTEMPTS:
        attempt += 1

        result = _request_fees(sku, asin, price)
        last_result = result

        if isinstance(result, dict) and "errors" in result:
            return result

        if result is None:
            continue

        net = result.get("net") or {}
        if net.get("ReferralFees") or net.get("FBAFees"):
            return result

    return last_result or {"errors": [{"code": "NoEstimate", "message": "No fees estimate returned"}]}


# ---------------------------------------------------------
# Helper: request + wait + download with retry
# ---------------------------------------------------------
async def fetch_active_listings_report():
    MAX_ATTEMPTS = 10

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}] [Active Listings] Attempt {attempt}/{MAX_ATTEMPTS}")

            report_id = request_report(
                "GET_MERCHANT_LISTINGS_DATA",
                params={"reportOptions": {"preferredReportDocumentLocale": "en_US"}}
            )

            if not report_id:
                raise RuntimeError("No reportId returned")

            doc_id = wait_for_report(report_id)
            text = download_report(doc_id)
            return text

        except Exception as e:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}] [Active Listings] Error: {e}")
            if attempt < MAX_ATTEMPTS:
                print(f"[{get_now_iso_string_with_custom_utc_offset()}] Waiting 60 seconds before retrying...")
                time.sleep(60)
            else:
                raise RuntimeError("Failed to retrieve Active Listings report after retries")


# ---------------------------------------------------------
# Main: refresh fee cache
# ---------------------------------------------------------
async def refresh_fee_cache():
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] === FeeEstimatesCache Refresh Started ===")

    listings_text = await fetch_active_listings_report()

    reader = csv.DictReader(StringIO(listings_text), delimiter="\t")

    active_items = []
    asin_list = set()

    for lr in reader:
        sku = (lr.get("seller-sku") or "").strip()
        asin = (lr.get("asin1") or "").strip()
        raw_price = lr.get("price")
        price = parse_cost(raw_price) if raw_price else None

        if sku and asin and price:
            active_items.append((sku, asin, price))
            asin_list.add(asin)

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Loaded {len(active_items)} active SKUs")

    if not active_items:
        print(f"[{get_now_iso_string_with_custom_utc_offset()}] No active listings found — aborting.")
        return

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Loading product details...")
    conn = connect_database()
    cursor = conn.cursor()
    product_details = get_product_details_by_asin(cursor, list(asin_list)) or {}
    cursor.close()
    conn.close()

    items_with_cog = []
    for sku, asin, price in active_items:
        d = product_details.get(asin) or {}
        cog = parse_cost(d.get("cost"))

        if cog is not None:
            items_with_cog.append((sku, asin, price, cog))

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] {len(items_with_cog)} SKUs have valid COG")

    if not items_with_cog:
        print(f"[{get_now_iso_string_with_custom_utc_offset()}] No SKUs with COG — aborting.")
        return

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Estimating fees for {len(items_with_cog)} SKUs...")

    fees = {}
    total = len(items_with_cog)

    for idx, (sku, asin, price, _) in enumerate(items_with_cog, start=1):
        if idx % 50 == 0 or idx == 1:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}]   Progress: {idx}/{total}")

        try:
            result = get_fees_estimate_local(sku, asin, price)
            fees[(sku, asin, price)] = result
        except Exception as e:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}] [ERROR] Fee estimate failed for {sku}: {e}")
            fees[(sku, asin, price)] = {}

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Computing financials...")
    cache_rows = []

    for (sku, asin, price, cog) in items_with_cog:
        resp = fees.get((sku, asin, price)) or {}
        net_block = resp.get("net") or {}

        ref = float(net_block.get("ReferralFees", 0) or 0)
        fba = float(net_block.get("FBAFees", 0) or 0)

        charges = ref + fba
        vat = price * GOVT_VAT_RATE
        net = price - charges - vat
        profit = net - cog

        cache_rows.append((
            sku,
            asin,
            price,
            ref,
            fba,
            charges,
            vat,
            net,
            cog,
            profit
        ))

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Prepared {len(cache_rows)} rows for DB")

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Saving to FeeEstimatesCache...")
    conn = connect_database()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    sql = """
        MERGE FeeEstimatesCache AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src
              (SKU, ASIN, Price, ReferralFee, FBAFee, Charges, VAT, Net, COG, Profit)
        ON target.SKU = src.SKU AND target.ASIN = src.ASIN
        WHEN MATCHED THEN
            UPDATE SET
                Price = src.Price,
                ReferralFee = src.ReferralFee,
                FBAFee = src.FBAFee,
                Charges = src.Charges,
                VAT = src.VAT,
                Net = src.Net,
                COG = src.COG,
                Profit = src.Profit,
                CachedAt = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (SKU, ASIN, Price, ReferralFee, FBAFee, Charges, VAT, Net, COG, Profit)
            VALUES (src.SKU, src.ASIN, src.Price, src.ReferralFee, src.FBAFee, src.Charges, src.VAT, src.Net, src.COG, src.Profit);
    """

    cursor.executemany(sql, cache_rows)
    conn.commit()

    cursor.close()
    conn.close()

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] === FeeEstimatesCache Refresh Complete ===")


if __name__ == "__main__":
    asyncio.run(refresh_fee_cache())