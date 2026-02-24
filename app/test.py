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
from bs4 import BeautifulSoup
from .database import connect_database
from .transactions import get_transactions

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


@router.get("/test-buybox")
async def test_pricing_raw(asin: str):
    """
    Test endpoint to fetch RAW pricing output for a given ASIN.
    Returns the exact SP-API payload for debugging.
    """

    try:
        params = {
            "MarketplaceId": MARKETPLACE_ID,
            "ItemCondition": "New"
        }

        # Call SP‑API
        resp = spapi_request(
            method="GET",
            path=f"/products/pricing/v0/items/{asin}/offers",
            params=params
        )

        return {
            "asin": asin,
            "raw_response": resp
        }

    except Exception as e:
        return {
            "asin": asin,
            "error": str(e)
        }


@router.get("/test-seller-name")
def test_seller_name(seller_id: str):

    """
    Fetch seller name from Amazon seller storefront page
    """

    import requests
    from bs4 import BeautifulSoup

    try:

        url = f"https://www.amazon.ae/sp?seller={seller_id}"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

        resp = requests.get(
            url,
            headers=headers,
            timeout=15
        )

        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Method 1 (most reliable)
        element = soup.select_one("#seller-name")

        if not element:

            # Method 2 (fallback)
            element = soup.select_one("h1")

        seller_name = element.get_text(strip=True) if element else None

        return {
            "seller_id": seller_id,
            "seller_name": seller_name,
            "status": "ok" if seller_name else "not_found"
        }

    except Exception as e:

        return {
            "seller_id": seller_id,
            "seller_name": None,
            "status": "error",
            "error": str(e)
        }

@router.get("/transactions/duplicate-transaction-ids")
def find_duplicate_transaction_ids(days: int = 15):
    """
    Detect duplicate TransactionId values returned by get_transactions().
    """

    # Match your main endpoint logic
    delta = timedelta(days=days)
    posted_after = format_dt_z(datetime.now(timezone.utc) - delta)
    params = {"postedAfter": posted_after}

    conn = connect_database()
    cursor = conn.cursor()

    try:
        data = get_transactions(params=params, db_cursor=cursor)
    finally:
        cursor.close()
        conn.close()

    # Group rows by TransactionId
    groups = {}
    for row in data:
        tid = row.get("TransactionId")
        if not tid:
            continue
        groups.setdefault(tid, []).append(row)

    # Keep only TransactionIds with more than one row
    duplicates = {
        tid: rows
        for tid, rows in groups.items()
        if len(rows) > 1
    }

    return {
        "total_rows": len(data),
        "duplicate_transaction_ids": len(duplicates),
        "duplicates": duplicates
    }


@router.get("/settlement-report/raw")
def get_latest_settlement_report():
    """
    Fetch the most recent Settlement Report V2 generated in the last 90 days.
    Settlement reports are usually bi-weekly, so a 90-day window ensures success.
    """
    import csv
    import gzip
    import io
    import requests
    import time
    from datetime import datetime, timedelta, timezone

    report_type = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2"

    # Step 1 — List reports from the last 90 days (Amazon's max retention)
    now_utc = datetime.now(timezone.utc)
    # 2 days is too short because settlements happen every 7-14 days.
    created_since = (now_utc - timedelta(days=90)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "reportTypes": report_type,
        "processingStatuses": "DONE",
        "pageSize": 1,  # We just want the most recent finalized report
        "createdSince": created_since,
        "marketplaceIds": [MARKETPLACE_ID]
    }

    print(f"Searching for {report_type} since {created_since}...")

    list_resp = spapi_request(
        method="GET",
        path="/reports/2021-06-30/reports",
        params=params
    )

    reports = list_resp.get("reports", [])
    if not reports:
        return {
            "error": "No settlement reports found. Check your disbursement cycle in Seller Central.",
            "createdSince": created_since,
            "api_response": list_resp
        }

    # Use the most recent generated report
    latest_report = reports[0]
    report_id = latest_report["reportId"]
    document_id = latest_report["reportDocumentId"]

    print(f"Found Report ID: {report_id} | Document ID: {document_id}")

    # Step 2 — Get the document download URL
    # We skip the "GET report by ID" call because document_id is already in the list response
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )

    url = doc_resp.get("url")
    compression = doc_resp.get("compressionAlgorithm")

    if not url:
        return {"error": "Could not retrieve download URL", "response": doc_resp}

    # Step 3 — Download the raw data
    print(f"Downloading report. Compression: {compression}")
    r = requests.get(url)
    raw_data = r.content

    # Step 4 — Decompress and decode
    if compression == "GZIP":
        try:
            raw_data = gzip.decompress(raw_data)
        except Exception as e:
            return {"error": f"Decompression failed: {str(e)}"}

    try:
        text_content = raw_data.decode("utf-8", errors="replace")
    except Exception as e:
        return {"error": f"Decoding failed: {str(e)}"}

    # Step 5 — Parse TAB-delimited file
    # Settlement reports use Tabs (\t), not commas.
    reader = csv.DictReader(io.StringIO(text_content), delimiter="\t")
    rows = list(reader)

    return {
        "info": {
            "reportId": report_id,
            "documentId": document_id,
            "generatedAt": latest_report.get("createdTime"),
            "rowCount": len(rows)
        },
        "allRows": rows  # This contains every transaction including 'Service Fees'
    }

@router.get("/transactions/raw-fba-reimbursements-5-days")
def get_fba_inventory_reimbursements_last_5_days():
    """
    Fetch Finances 2024‑06‑19 transactions for the last 5 days.
    Filter ONLY FBAInventoryReimbursement transactions.
    Return raw processed rows.
    """

    from datetime import datetime, timedelta, timezone

    now_utc = datetime.now(timezone.utc)

    posted_after = (now_utc - timedelta(days=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    posted_before = (now_utc - timedelta(minutes=3)).strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
        "pageSize": 100
    }

    print("Fetching Finances 2024‑06‑19 transactions (FBAInventoryReimbursement only)...")
    print("PostedAfter:", posted_after)
    print("PostedBefore:", posted_before)

    # DB cursor required by get_transactions()
    conn = connect_database()
    cur = conn.cursor()

    try:
        all_rows = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    # ⭐ Filter only FBAInventoryReimbursement
    filtered = [
        row for row in all_rows
        if (row.get("TransactionType") or "").strip() == "FBAInventoryReimbursement"
    ]

    return {
        "posted_after": posted_after,
        "posted_before": posted_before,
        "total_rows": len(all_rows),
        "fba_reimbursement_rows": len(filtered),
        "preview": filtered[:20],
        "rows": filtered
    }

@router.get("/debug/transactions/duplicate-transaction-ids")
def debug_duplicate_transaction_ids():
    """
    Fetches processed transactions (via get_transactions) for last 3 days
    and returns only TransactionIds that appear more than once.
    """

    # Last 3 days window
    delta = timedelta(days=3)
    posted_after = format_dt_z(datetime.now(timezone.utc) - delta)

    params = {
        "postedAfter": posted_after
    }

    conn = connect_database()
    cur = conn.cursor()

    try:
        # Use your main processor (includes item expansion + product mapping)
        rows = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    # Group by TransactionId
    groups = {}
    for row in rows:
        tid = row.get("TransactionId")
        if not tid:
            continue
        groups.setdefault(tid, []).append(row)

    # Keep only duplicates
    duplicates = {
        tid: tx_rows
        for tid, tx_rows in groups.items()
        if len(tx_rows) > 1
    }

    return {
        "posted_after": posted_after,
        "total_rows": len(rows),
        "duplicate_transaction_ids": len(duplicates),
        "duplicates": duplicates,
    }