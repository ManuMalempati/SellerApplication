from fastapi import APIRouter
from datetime import datetime, timedelta, timezone
from .auth import spapi_request
import config
from app.utilities.utils import convert_utc_to_utcz_string
from urllib.parse import quote
from config import MARKETPLACE_ID, BASE_CURRENCY_CODE
import json

router = APIRouter()

@router.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = convert_utc_to_utcz_string(datetime.now(timezone.utc) - delta)

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    return spapi_request("GET", "/orders/v0/orders", params=params)

@router.get("/order-items")
async def get_order():
    orderId = "S02-3960449-6844843"
    return spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")

from fastapi import Query

# ... (your existing /raw-orders and /order-items endpoints)

@router.get("/test-fee-estimate")
async def test_fee_estimate(
    sku: str = Query(None), 
    asin: str = Query(None), 
    price: float = Query(...)
):
    """
    Fetch the RAW SP‑API fee estimate response for a given SKU or ASIN.
    Usage:
      /test-fee-estimate?sku=AD80HW-3&price=384
      /test-fee-estimate?asin=B07MX51R53&price=15.75
    """

    if not sku and not asin:
        return {"error": "Provide either sku= or asin="}

    # 1. Define common request body
    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": config.MARKETPLACE_ID,
            "IsAmazonFulfilled": True, # Assuming FBA for testing
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": config.BASE_CURRENCY_CODE,
                    "Amount": price
                }
            },
            "Identifier": f"{(sku or asin)}-test-estimate"
        }
    }

    # 2. Determine Endpoint and Path
    # Note: Listings endpoint requires the SKU to be quoted (URL encoded)
    if sku:
        safe_sku = quote(sku, safe="")
        path = f"/products/fees/v0/listings/{safe_sku}/feesEstimate"
    else:
        path = f"/products/fees/v0/items/{asin}/feesEstimate"

    # 3. Call SP‑API
    try:
        resp = spapi_request(
            method="POST",
            path=path,
            body=body
        )
        
        return {
            "status": "success",
            "request_info": {
                "sku": sku,
                "asin": asin,
                "price": price,
                "currency": config.BASE_CURRENCY_CODE,
                "marketplace": config.MARKETPLACE_ID
            },
            "raw_response": resp
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": str(e)
        }


@router.get("/test_batch_fee_estimate")
def test_batch_fees():
    skus = [
        "AD80HW-3",
        "SDG4/128GB",
        "SDCZ430-128G-G46",
        "P-SDUX512U3100PRO-GE",
        "SDSSDE81-1T00-G25",
        "SDSSDE81-4T00-RR25",
        "LSL500X001T-RNBNG",
        "HDTB540EK3CA-1",
        "0B47062",
        "SDSSDE61-2T00-G25",
        "SDSDUNC-256G-GN6IN",
        "SDSQUA4-032G-GN6MN"
    ]

    TEST_PRICE = 384.00  # Amazon requires a price

    # EXACT structure required by getMyFeesEstimates
    requests_list = []

    for i, sku in enumerate(skus, start=1):
        requests_list.append({
            "IdType": "SellerSKU",
            "IdValue": sku,
            "FeesEstimateRequest": {
                "MarketplaceId": MARKETPLACE_ID,
                "Identifier": str(i),
                "IsAmazonFulfilled": True,
                "PriceToEstimateFees": {
                    "ListingPrice": {
                        "Amount": TEST_PRICE,
                        "CurrencyCode": BASE_CURRENCY_CODE
                    }
                }
            }
        })

    body = requests_list  # IMPORTANT: root is an ARRAY, not an object

    print("\n=== SENDING BATCH REQUEST BODY ===")
    print(json.dumps(body, indent=2))

    resp = spapi_request(
        method="POST",
        path="/products/fees/v0/feesEstimate",
        body=body
    )

    print("\n=== RAW AMAZON RESPONSE ===")
    print(json.dumps(resp, indent=2))

    return resp


if __name__ == "__main__":
    test_batch_fees()