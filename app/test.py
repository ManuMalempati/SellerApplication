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
    orderId = "408-7212017-2853114"
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
    asin = "B07HHD7C7T"
    marketplace_id = os.getenv("MARKETPLACE_ID")
    currency_code = os.getenv("BASE_CURRENCY_CODE", "AED")

    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency_code,
                    "Amount": 58.68
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

@router.get("/test-raw-report")
async def test_raw_report(hours: int = 200):
    """
    Fetch raw rows from GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL
    and return ONLY orders that appear multiple times with different statuses.
    """

    # 1. Compute window
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(hours=hours)

    print(f"Requesting report for {start_dt.isoformat()} to {end_dt.isoformat()}")

    # 2. Create report
    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        return {"error": "Failed to create report", "response": create_resp}

    report_id = create_resp["reportId"]

    # 3. Poll until DONE
    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )
        if status_resp and status_resp.get("processingStatus") == "DONE":
            break
        await asyncio.sleep(5)
    else:
        return {"error": "Timeout waiting for report"}

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        return {"error": "No reportDocumentId", "response": status_resp}

    # 4. Get download URL
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )
    if not doc_resp or "url" not in doc_resp:
        return {"error": "Failed to get document URL", "response": doc_resp}

    url = doc_resp["url"]

    # 5. Download the file
    raw = requests.get(url).content
    compression = doc_resp.get("compressionAlgorithm")

    if compression == "GZIP":
        decoded = gzip.decompress(raw).decode("utf-8")
    else:
        decoded = raw.decode("utf-8")

    # 6. Parse TSV → JSON
    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    # ---------------------------------------------------------
    # 7. Detect orders with multiple statuses inside the report
    # ---------------------------------------------------------
    status_map = {}  # orderId -> set(statuses)
    row_map = {}     # orderId -> list(rows)

    for r in rows:
        oid = r.get("amazon-order-id")
        status = r.get("order-status")

        if not oid:
            continue

        status_map.setdefault(oid, set()).add(status)
        row_map.setdefault(oid, []).append(r)

    # Orders where the same orderId has >1 distinct statuses
    changed_orders = {
        oid: row_map[oid]
        for oid, statuses in status_map.items()
        if len(statuses) > 1
    }

    return {
        "count": len(changed_orders),
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
        "changed_orders": changed_orders,
    }
