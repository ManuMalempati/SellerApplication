# estimates.py
from .auth import spapi_request
from urllib.parse import quote
import os

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
FEES_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", 1.0))

def _extract_fees(response):
    if "errors" in response:
        # Return the error code so get_fees_estimate can decide to fallback
        return response.get("errors")[0].get("code")

    result_root = response.get("payload", {}).get("FeesEstimateResult", {})
    if result_root.get("Status") != "Success":
        return None

    FeesEstimate = result_root["FeesEstimate"]
    total = FeesEstimate["TotalFeesEstimate"]["Amount"]
    currency = FeesEstimate["TotalFeesEstimate"]["CurrencyCode"]

    referral = 0
    fba = 0
    for fee in FeesEstimate["FeeDetailList"]:
        if fee["FeeType"] == "ReferralFee":
            referral = fee["FinalFee"]["Amount"]
        elif fee["FeeType"].startswith("FBA"):
            fba += fee["FinalFee"]["Amount"]

    return {
        "CurrencyCode": currency,
        "TotalAmazonFees": total * FEES_VAT_MULTIPLIER,
        "ReferralFees": referral * FEES_VAT_MULTIPLIER,
        "FBAFees": fba * FEES_VAT_MULTIPLIER
    }

def get_fees_estimate(sku, asin, price):
    # 1. Attempt SKU-based
    sku_body = {
        "FeesEstimateRequest": {
            "MarketplaceId": MARKETPLACE_ID, "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {"ListingPrice": {"CurrencyCode": BASE_CURRENCY_CODE, "Amount": price}},
            "Identifier": f"{sku}-est"
        }
    }
    # Encode SKU for the URL path
    safe_sku = quote(sku)
    sku_resp = spapi_request("POST", f"/products/fees/v0/listings/{safe_sku}/feesEstimate", body=sku_body)
    res = _extract_fees(sku_resp)

    # If successful, return. If QuotaExceeded, return that string to trigger retry in orders.py
    if isinstance(res, dict): return res
    if res == "QuotaExceeded": return {"errors": [{"code": "QuotaExceeded"}]}

    # 2. Fallback to ASIN (if SKU failed or returned Unauthorized)
    asin_body = {
        "FeesEstimateRequest": {
            "MarketplaceId": MARKETPLACE_ID, "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {"ListingPrice": {"CurrencyCode": BASE_CURRENCY_CODE, "Amount": price}},
            "Identifier": f"{asin}-est"
        }
    }
    asin_resp = spapi_request("POST", f"/products/fees/v0/items/{asin}/feesEstimate", body=asin_body)
    final_res = _extract_fees(asin_resp)
    
    if isinstance(final_res, str): # Error code string
        return {"errors": [{"code": final_res}]}
    return final_res