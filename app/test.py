from fastapi import APIRouter
from datetime import datetime, timedelta, timezone
from .auth import spapi_request
import config
from .utils import convert_utc_to_utcz_string
from urllib.parse import quote

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
    orderId = "406-3986866-3793910"
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
      /test-fee-estimate?sku=0B36404&price=878
      /test-fee-estimate?asin=B07H4PR6HN&price=878.0
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
