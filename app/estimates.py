from .auth import spapi_request
import os

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
FEES_VAT_MULTIPLIER = float(os.getenv("GOVT_VAT_RATE_DIVISOR"))/(float(os.getenv("GOVT_VAT_RATE_DIVISOR")) -1 )

def get_fees_estimate(asin, price):

    # Build the body of the request based on given ASIN and ListingPrice
    body = {
        "FeesEstimateRequest":{
            "MarketplaceId": MARKETPLACE_ID,
            "IdType": "ASIN", 
            "IdValue": asin,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees":{
                "ListingPrice": {
                "CurrencyCode": BASE_CURRENCY_CODE,
                "Amount": price
                }
            },
            "Identifier": f"{asin}-estimate"
        }
    }

    # ASIN goes in as path parameter
    response = spapi_request(method="POST", path=f"/products/fees/v0/items/{asin}/feesEstimate", body=body)

    result_root = response.get("payload", {}).get("FeesEstimateResult", {}) 
    if result_root.get("Status") != "Success": 
        return None

    ReferralFees = 0
    FBAFees = 0
    TotalAmazonFees = 0

    FeesEstimate = result_root["FeesEstimate"]
    TotalAmazonFees = FeesEstimate["TotalFeesEstimate"]["Amount"]
    CurrencyCode = FeesEstimate["TotalFeesEstimate"]["CurrencyCode"]
    for fee in FeesEstimate["FeeDetailList"]:
        fee_type = fee["FeeType"]
        amount = fee["FinalFee"]["Amount"]
        if(fee_type == "ReferralFee"):
            ReferralFees = amount
        elif (fee_type.startswith("FBA")):
            FBAFees += amount
        
    # Add Tax since SP API result does not incl tax
    TotalAmazonFees *= FEES_VAT_MULTIPLIER
    ReferralFees *= FEES_VAT_MULTIPLIER
    FBAFees *= FEES_VAT_MULTIPLIER

    # This is a function for internal use, so no need to add negatives, return raw result
    result = {"CurrencyCode": CurrencyCode, "TotalAmazonFees": TotalAmazonFees, "ReferralFees": ReferralFees, "FBAFees": FBAFees}

    return result