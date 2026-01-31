from .auth import spapi_request
from .database import get_all_product_mapping, connect_database
import os
import time

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
LAST_CALL = 0

def chunk_list(lst, size):
    """Yield successive chunks of size 'size' from list."""
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

def get_FBA_STOCK(summary):
    stock = summary.get("inventoryDetails", {}).get("fulfillableQuantity", 0)
    return stock

def throttle():
    global LAST_CALL
    now = time.time()
    elapsed = now - LAST_CALL
    # only one request every 0.5 seconds
    if(elapsed < 0.5):
        time.sleep(0.5 - elapsed)
    
    LAST_CALL = time.time()

def get_listing_prices_batch(sku_list):
    unique_skus = list(set(sku_list))
    prices = {}

    for chunk in chunk_list(unique_skus, 20):
        throttle()  # reuse your existing throttle

        resp = spapi_request(
            "GET",
            "/products/pricing/v0/price",
            params={
                "MarketplaceId": MARKETPLACE_ID,
                "Skus": ",".join(chunk),
                "ItemType": "Sku",
                "ItemCondition": "New",
            },
        )

        for item in resp.get("payload", []):
            sku = item.get("SellerSKU")
            offers = item.get("Product", {}).get("Offers", [])
            if offers:
                amt = offers[0].get("BuyingPrice", {}).get("ListingPrice", {}).get("Amount")
                if amt is not None:
                    prices[sku] = float(amt)

    return prices


def buyboxes():
    conn = connect_database()
    cursor = conn.cursor()

    # Load SKUs from ProductMapping
    product_mappings = get_all_product_mapping(cursor)
    all_skus = list(product_mappings.keys())

    cursor.close()
    conn.close()

    rows = []

    all_summaries = []

    # Process SKUs in batches of 50
    for batch in chunk_list(all_skus, 50):

        # wait if API rate limits are hit
        throttle()

        params = {
            "granularityType": "Marketplace",
            "granularityId": MARKETPLACE_ID,
            "marketplaceIds": [MARKETPLACE_ID],
            "sellerSkus": batch,
            "details": True
        }

        response = spapi_request(
            method="GET",
            path="/fba/inventory/v1/summaries",
            params=params
        )

        # Extract summaries safely
        payload = response.get("payload", {})
        summaries = payload.get("inventorySummaries", [])

        all_summaries.extend(summaries)

        next_token = response.get("pagination", {}).get("nextToken")
        # while we are given next_token, keep paginating
        while next_token:
            throttle()

            response = spapi_request(
                method="GET",
                path="/fba/inventory/v1/summaries",
                params={"nextToken": next_token}
            )

            # Extract summaries safely
            payload = response.get("payload", {})
            summaries = payload.get("inventorySummaries", [])

            all_summaries.extend(summaries)

            next_token = response.get("pagination", {}).get("nextToken")

    for summary in all_summaries:
        fba_stock = get_FBA_STOCK(summary)
        if(fba_stock > 0):
            row = {}
            row["asin"] = summary.get("asin")
            row["sku"] = summary.get("sellerSku")
            row["ssku"] = product_mappings.get(summary.get("sellerSku")).get("ssku")
            row["fnsku"] = summary.get("fnSku")
            row["FBA-Stock"] = fba_stock
            rows.append(row)

    return rows
