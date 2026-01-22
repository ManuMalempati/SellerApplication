from datetime import datetime, timedelta
from .main import app, connection
from .transactions import get_transactions
from .estimates import get_fees_estimate
from .auth import spapi_request
from datetime import datetime, timedelta

@app.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    response = spapi_request("GET", "/orders/v0/orders", params=params)

    return response

@app.get("/order")
async def get_order():

    orderId = "407-9307761-1723560"

    response = spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")

    return response

import os
from .auth import spapi_request # Assuming this is where your request helper lives

@app.get("/test-pricing")
async def test_get_pricing(item_type: str = "Asin"):
    """
    Test endpoint for SP-API getPricing
    item_type: 'Asin' or 'Sku'
    """
    
    # 1. Setup identifiers (Replace these with real ones from your inventory to test)
    test_asins = ["B0080W1VVC", "B07SHGW6VL"]
    test_skus = ["SDSSDE61-2T00-G25", "STKM2000400"]
    
    # 2. Build parameters
    # Note: SP-API expects arrays in GET requests as comma-separated strings
    params = {
        "MarketplaceId": os.getenv("MARKETPLACE_ID"),
        "ItemType": item_type,
    }
    
    if item_type == "Asin":
        params["Asins"] = ",".join(test_asins)
    else:
        params["Skus"] = ",".join(test_skus)

    # 3. Optional filters
    # params["ItemCondition"] = "New"
    # params["OfferType"] = "B2C"

    # 4. Make the request
    response = spapi_request(
        method="GET", 
        path="/products/pricing/v0/price", 
        params=params
    )

    return response


@app.get("/test-fees")
async def test_get_fees():
    """
    Test endpoint for SP-API getMyFeesEstimateForASIN
    """
    # 1. Setup identifier (Replace with a real ASIN from your inventory)
    asin = "B017RD0WHS"
    
    # 2. Get environment settings
    marketplace_id = os.getenv("MARKETPLACE_ID")
    currency_code = os.getenv("BASE_CURRENCY_CODE", "AED") # Default to AED if not set

    # 3. Construct the Request Body (Required by SP-API)
    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "IsAmazonFulfilled": True, # Set to True for FBA estimates
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency_code,
                    "Amount": 100.00 # The hypothetical price you want to test
                }
            },
            "Identifier": f"{asin}-estimate",
        }
    }

    # 4. Make the request (Must be POST)
    response = spapi_request(
        method="POST", 
        path=f"/products/fees/v0/items/{asin}/feesEstimate",
        body=body
    )

    return response