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


REPORT_TYPE = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL"

@router.get("/test-orders-last-2-days")
async def test_orders_last_2_days():
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=2)

    # 1. Request the all orders report
    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": REPORT_TYPE,
            "dataStartTime": start.isoformat(),
            "dataEndTime": now.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID]
        }
    )
    # Defensive: Check for errors!
    if not create_resp or "reportId" not in create_resp:
        return {
            "status": "error",
            "message": "Failed to create report.",
            "details": create_resp
        }

    report_id = create_resp["reportId"]

    # 2. Poll until report is DONE (with max timeout)
    max_attempts = 60
    for attempt in range(max_attempts):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )
        if not status_resp:
            await asyncio.sleep(3)
            continue
        processing_status = status_resp.get("processingStatus")
        if processing_status == "DONE":
            break
        elif processing_status in ("CANCELLED", "FATAL"):
            return {
                "status": "error",
                "message": f"Report processing failed or cancelled: {processing_status}",
                "details": status_resp
            }
        await asyncio.sleep(5)
    else:
        return {
            "status": "error",
            "message": "Timeout waiting for report to be DONE.",
            "last_status": status_resp
        }

    # 3. Get document ID
    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        return {
            "status": "error",
            "message": "No reportDocumentId found in finished report.",
            "details": status_resp
        }

    # 4. Download the document
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )
    if not doc_resp or "url" not in doc_resp:
        return {
            "status": "error",
            "message": "Failed to get download URL.",
            "details": doc_resp
        }

    import requests
    url = doc_resp["url"]
    raw = requests.get(url).content

    # 5. Parse TSV
    compression = doc_resp.get("compressionAlgorithm")
    if compression == "GZIP":
        import gzip
        decoded = gzip.decompress(raw).decode("utf-8")
    else:
        decoded = raw.decode("utf-8")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    return {
        "status": "ok",
        "count": len(rows),
        "columns": reader.fieldnames,
        "rows": rows  # Return ALL rows (could return rows[:100] for sampling if too large)
    }