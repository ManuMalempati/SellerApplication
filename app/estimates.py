from .auth import spapi_request
import os

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
FEES_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER"))


def _extract_fees(response):
    """
    Internal helper to extract fees from a valid SP-API response.
    """
    result_root = response.get("payload", {}).get("FeesEstimateResult", {})
    if result_root.get("Status") != "Success":
        return None

    FeesEstimate = result_root["FeesEstimate"]

    total = FeesEstimate["TotalFeesEstimate"]["Amount"]
    currency = FeesEstimate["TotalFeesEstimate"]["CurrencyCode"]

    referral = 0
    fba = 0

    for fee in FeesEstimate["FeeDetailList"]:
        fee_type = fee["FeeType"]
        amount = fee["FinalFee"]["Amount"]

        if fee_type == "ReferralFee":
            referral = amount
        elif fee_type.startswith("FBA"):
            fba += amount

    # Apply VAT
    total *= FEES_VAT_MULTIPLIER
    referral *= FEES_VAT_MULTIPLIER
    fba *= FEES_VAT_MULTIPLIER

    return {
        "CurrencyCode": currency,
        "TotalAmazonFees": total,
        "ReferralFees": referral,
        "FBAFees": fba
    }


def get_fees_estimate(sku, asin, price):
    """
    Combined estimator:
    1. Try SKU-based fee estimation (most accurate)
    2. If SKU fails, fallback to ASIN-based estimation
    """

    # --- Attempt SKU-based estimation ---
    sku_body = {
        "FeesEstimateRequest": {
            "MarketplaceId": MARKETPLACE_ID,
            "IdType": "SKU",
            "IdValue": sku,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": BASE_CURRENCY_CODE,
                    "Amount": price
                }
            },
            "Identifier": f"{sku}-estimate"
        }
    }

    sku_response = spapi_request(
        method="POST",
        path=f"/products/fees/v0/listings/{sku}/feesEstimate",
        body=sku_body
    )

    sku_fees = _extract_fees(sku_response)

    if sku_fees:
        sku_fees["Source"] = "SKU"
        return sku_fees

    # --- Fallback to ASIN-based estimation ---
    asin_body = {
        "FeesEstimateRequest": {
            "MarketplaceId": MARKETPLACE_ID,
            "IdType": "ASIN",
            "IdValue": asin,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": BASE_CURRENCY_CODE,
                    "Amount": price
                }
            },
            "Identifier": f"{asin}-estimate"
        }
    }

    asin_response = spapi_request(
        method="POST",
        path=f"/products/fees/v0/items/{asin}/feesEstimate",
        body=asin_body
    )

    asin_fees = _extract_fees(asin_response)

    if asin_fees:
        asin_fees["Source"] = "ASIN"
        return asin_fees

    # If both fail
    return None
