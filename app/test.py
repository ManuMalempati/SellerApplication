from fastapi import APIRouter
from datetime import datetime, timedelta, timezone
from .auth import spapi_request
import os
import time
import gzip
import csv
import requests
import io
import asyncio

router = APIRouter()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

def format_dt_z(d: datetime) -> str:
    """Return canonical UTC Z timestamp like 2026-01-26T05:48:16Z."""
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

@router.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = format_dt_z(datetime.now(timezone.utc) - delta)

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    return spapi_request("GET", "/orders/v0/orders", params=params)

@router.get("/order-items")
async def get_order():
    orderId = "406-3986866-3793910"
    return spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")

@router.get("/test-pricing")
async def test_get_pricing(item_type: str = "Asin"):
    test_asins = ["B07NNRTTCM"]
    test_skus = ["SDSSDE61-2T00-G25", "STKM2000400"]

    params = {
        "MarketplaceId": os.getenv("MARKETPLACE_ID"),
        "ItemType": item_type,
    }

    if item_type == "Asin":
        params["Asins"] = ",".join(test_asins)
    else:
        params["Skus"] = ",".join(test_skus)

    return spapi_request("GET", "/products/pricing/v0/price", params=params)

@router.get("/test-fees")
async def test_get_fees():
    asin = "B0842P5GBQ"
    marketplace_id = os.getenv("MARKETPLACE_ID")
    currency_code = os.getenv("BASE_CURRENCY_CODE", "AED")

    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency_code,
                    "Amount": 69
                }
            },
            "Identifier": f"{asin}-estimate",
        }
    }

    return spapi_request(
        "POST",
        f"/products/fees/v0/items/{asin}/feesEstimate",
        body=body
    )


# @router.get("/buybox")
# async def buybox():
#     sku = "0628C002AA"
#     params = {"ItemCondition": "New"}
#     params["MarketplaceId"] = MARKETPLACE_ID
#     result = spapi_request(method="GET", path=f"/products/pricing/v0/listings/{sku}/offers", params=params)
#     return result

@router.get("/test-pricing-raw")
async def test_pricing_raw(
    sku: str = None,
    asin: str = None,
    item_type: str = "Sku"
):
    """
    Test endpoint to fetch RAW pricing output exactly like buyboxes() does.
    Example:
      /test-pricing-raw?sku=BL.9BWWA.559
      /test-pricing-raw?asin=B0CMCFGWK6&item_type=Asin
    """

    sku = 'KS63NMUSBL00'
    asin = 'B0CMCFGWK6'
    if not sku and not asin:
        return {"error": "Provide either sku= or asin="}

    params = {
        "MarketplaceId": MARKETPLACE_ID,
        "ItemType": item_type,
    }

    if item_type.lower() == "sku":
        params["Skus"] = sku
    else:
        params["Asins"] = asin

    # Call SP‑API exactly like buyboxes() does
    resp = spapi_request(
        "GET",
        "/products/pricing/v0/price",
        params=params
    )

    return {
        "input": {"sku": sku, "asin": asin, "item_type": item_type},
        "raw_response": resp
    }

def throttle():
    time.sleep(0.5)

@router.get("/test-sales-traffic-filtered")
def test_sales_traffic_filtered():

    # -----------------------------
    # 1. Build L-30 date range
    # -----------------------------
    end = datetime.utcnow()
    start = end - timedelta(days=30)

    dataStartTime = start.strftime("%Y-%m-%dT00:00:00Z")
    dataEndTime = end.strftime("%Y-%m-%dT00:00:00Z")

    print(f"Requesting L-30 days: {dataStartTime} -> {dataEndTime}")

    # -----------------------------
    # 2. Request the report
    # -----------------------------
    throttle()
    body = {
        "reportType": "GET_SALES_AND_TRAFFIC_REPORT",
        "dataStartTime": dataStartTime,
        "dataEndTime": dataEndTime,
        "reportOptions": {
            "dateGranularity": "DAY",
            "asinGranularity": "CHILD"
        },
        "marketplaceIds": [MARKETPLACE_ID]
    }

    resp = spapi_request(
        "POST",
        "/reports/2021-06-30/reports",
        body=body
    ) or {}

    report_id = resp.get("reportId")
    if not report_id:
        return {"error": "No reportId returned", "response": resp}

    print(f"Report requested: {report_id}")

    # -----------------------------
    # 3. Poll until DONE
    # -----------------------------
    while True:
        throttle()
        status_resp = spapi_request(
            "GET",
            f"/reports/2021-06-30/reports/{report_id}"
        ) or {}

        status = status_resp.get("processingStatus")
        print(f"Status: {status}")

        if status == "DONE":
            document_id = status_resp.get("reportDocumentId")
            break

        time.sleep(2)

    print(f"Report ready: {document_id}")

    # -----------------------------
    # 4. Download the JSON
    # -----------------------------
    throttle()
    doc = spapi_request(
        "GET",
        f"/reports/2021-06-30/documents/{document_id}"
    ) or {}

    url = doc.get("url")
    raw = requests.get(url).content

    if doc.get("compressionAlgorithm") == "GZIP":
        import gzip
        raw = gzip.decompress(raw)

    report_json = raw.decode("utf-8")
    import json
    data = json.loads(report_json)

    # -----------------------------
    # 5. Extract L-30 totals per ASIN
    # -----------------------------
    asin_rows = data.get("salesAndTrafficByAsin", [])

    dedup = {}  # ⭐ dict keyed by ASIN

    for row in asin_rows:
        asin = row.get("parentAsin") or row.get("childAsin")
        if not asin:
            continue

        sales = row.get("salesByAsin", {})
        traffic = row.get("trafficByAsin", {})

        dedup[asin] = {
            "ASIN": asin,
            "TotalOrderItems_L30": sales.get("totalOrderItems", 0),
            "OrderedProductSales_L30": sales.get("orderedProductSales", 0),
            "UnitsRefunded_L30": sales.get("unitsRefunded", 0),
            "BuyBoxPercentage_L30": traffic.get("buyBoxPercentage", 0)
        }

    results = list(dedup.values())

    print(f"Extracted {len(results)} ASIN rows (after ASIN + dedupe filter)")

    return {
        "count": len(results),
        "items": results
    }

@router.get("/test-reserved")
def test_reserved_inventory(ssku: str):
    """
    Test endpoint to verify get_reserved_inventory_by_ssku().
    Compares:
      1) Function output
      2) Direct SQL query result
    """
    from .database import connect_database, get_reserved_inventory_by_ssku

    print(f"\n=== TEST: Reserved Inventory Lookup for SSKU = {ssku} ===")

    conn = connect_database()
    cursor = conn.cursor()

    try:
        # 1) Test the function
        func_result = get_reserved_inventory_by_ssku(cursor, [ssku])
        print("Function returned:", func_result)

        # 2) Direct SQL query
        cursor.execute("""
            SELECT PartNumber, TotalStock
            FROM spapi_app_user.CurrentInventory
            WHERE PartNumber = ?
        """, (ssku,))
        row = cursor.fetchone()

        if row:
            direct_result = {"PartNumber": row[0], "TotalStock": row[1]}
        else:
            direct_result = None

        print("Direct SQL result:", direct_result)

        return {
            "ssku": ssku,
            "function_result": func_result,
            "direct_sql_result": direct_result
        }

    except Exception as e:
        return {"error": str(e)}

    finally:
        cursor.close()
        conn.close()
