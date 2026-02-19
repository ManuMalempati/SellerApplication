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


from .fba.sales_traffic import fetch_l30_sales_traffic
from .database import connect_database
@router.get("/test-l30")
def test_l30(asin: str = None):
    """
    Test endpoint to inspect L-30 Sales & Traffic data.
    Also checks how many ASINs from FBAProductSummary
    appear in the L30 report.
    """
    # 1. Fetch L30 data from Amazon
    l30_data = fetch_l30_sales_traffic()
    l30_asins = set(l30_data.keys())

    # 2. Load ASINs from FBAProductSummary
    conn = connect_database()
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT asin FROM FBAProductSummary WHERE asin IS NOT NULL")
    pm_asins = {row[0] for row in cursor.fetchall()}
    cursor.close()
    conn.close()

    # 3. Compute intersection
    overlap = l30_asins.intersection(pm_asins)

    # 4. If user requested a specific ASIN
    if asin:
        asin = asin.strip()
        return {
            "requested_asin": asin,
            "exists_in_l30": asin in l30_asins,
            "exists_in_product_mapping": asin in pm_asins,
            "l30_value": l30_data.get(asin),
            "total_l30_asins": len(l30_asins),
            "total_pm_asins": len(pm_asins),
            "overlap_count": len(overlap),
        }

    # 5. Default summary response
    return {
        "total_l30_asins": len(l30_asins),
        "total_pm_asins": len(pm_asins),
        "overlap_count": len(overlap),
        "sample_overlap": list(overlap)[:20],
        "l30_sample": {k: l30_data[k] for k in list(l30_asins)[:10]},
    }

@router.get("/test-active-listings")
def test_active_listings():
    """
    Requests the Active Listings Report (GET_MERCHANT_LISTINGS_DATA)
    and returns ALL rows, filtered to the important attributes only.
    """

    # -----------------------------
    # 1. Request the report
    # -----------------------------
    body = {
        "reportType": "GET_MERCHANT_LISTINGS_DATA",
        "marketplaceIds": [MARKETPLACE_ID],
        "reportOptions": {
            "preferredReportDocumentLocale": "en_US"
        }
    }

    print("[active-listings] Requesting report...")
    resp = spapi_request(
        "POST",
        "/reports/2021-06-30/reports",
        body=body
    ) or {}

    report_id = resp.get("reportId")
    if not report_id:
        return {"error": "No reportId returned", "response": resp}

    print(f"[active-listings] Report requested: {report_id}")

    # -----------------------------
    # 2. Poll until DONE
    # -----------------------------
    while True:
        time.sleep(1)
        status_resp = spapi_request(
            "GET",
            f"/reports/2021-06-30/reports/{report_id}"
        ) or {}

        status = status_resp.get("processingStatus")
        print(f"[active-listings] Status: {status}")

        if status == "DONE":
            document_id = status_resp.get("reportDocumentId")
            break

        if status in ("CANCELLED", "FATAL"):
            return {"error": "Report failed", "status": status_resp}

    print(f"[active-listings] Report ready: {document_id}")

    # -----------------------------
    # 3. Download the document
    # -----------------------------
    doc = spapi_request(
        "GET",
        f"/reports/2021-06-30/documents/{document_id}"
    ) or {}

    url = doc.get("url")
    raw = requests.get(url).content

    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)

    # -----------------------------
    # 4. Parse the TSV
    # -----------------------------
    text = raw.decode("utf-8", errors="replace")
    reader = csv.DictReader(io.StringIO(text), delimiter="\t")

    rows = list(reader)
    print(f"[active-listings] Parsed {len(rows)} rows")

    # -----------------------------
    # 5. Filter to important attributes ONLY
    # -----------------------------
    filtered = []
    for r in rows:
        filtered.append({
            "item-name": r.get("item-name"),
            "listing-id": r.get("listing-id"),
            "seller-sku": r.get("seller-sku"),
            "price": r.get("price"),
            "quantity": r.get("quantity"),
            "fulfillment-channel": r.get("fulfillment-channel"),
            "item-condition": r.get("item-condition"),
        })

    # -----------------------------
    # 6. Return ONE list only
    # -----------------------------
    return filtered

@router.get("/test-returns-raw-full")
async def test_returns_raw_full(days: int = 30):
    """
    Fetch GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE for last X days
    and return the full raw text exactly as Amazon sends it.
    """

    from datetime import datetime, timedelta, timezone
    import gzip, requests

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    print(f"[returns-raw-full] Requesting report for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    # 1. Create report
    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        return {"error": "Failed to create report", "response": create_resp}

    report_id = create_resp["reportId"]
    print(f"[returns-raw-full] Report requested: {report_id}")

    # 2. Poll until DONE
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

    print(f"[returns-raw-full] Document ready: {document_id}")

    # 3. Get download URL
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )
    if not doc_resp or "url" not in doc_resp:
        return {"error": "Failed to get document URL", "response": doc_resp}

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print(f"[returns-raw-full] Downloading raw document...")

    # 4. Download raw bytes
    raw_bytes = requests.get(url).content
    raw_len = len(raw_bytes)

    # 5. Decode raw text
    try:
        if compression == "GZIP":
            decoded = gzip.decompress(raw_bytes).decode("utf-8", errors="replace")
        else:
            decoded = raw_bytes.decode("utf-8", errors="replace")
    except Exception as e:
        decoded = f"[decode error: {e}]"

    return {
        "compression": compression,
        "raw_bytes_length": raw_len,
        "raw_text": decoded,
        "start": start_dt.isoformat(),
        "end": end_dt.isoformat(),
    }
