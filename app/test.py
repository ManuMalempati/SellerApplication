from fastapi import APIRouter
from datetime import datetime, timedelta
from .auth import spapi_request
import os
import time
import gzip
import csv
import requests

router = APIRouter()

@router.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    return spapi_request("GET", "/orders/v0/orders", params=params)

@router.get("/order")
async def get_order():
    orderId = "403-1446819-7082744"
    return spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")

@router.get("/test-pricing")
async def test_get_pricing(item_type: str = "Asin"):
    test_asins = ["B0080W1VVC"]
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
    asin = "B017RD0WHS"
    marketplace_id = os.getenv("MARKETPLACE_ID")
    currency_code = os.getenv("BASE_CURRENCY_CODE", "AED")

    body = {
        "FeesEstimateRequest": {
            "MarketplaceId": marketplace_id,
            "IsAmazonFulfilled": True,
            "PriceToEstimateFees": {
                "ListingPrice": {
                    "CurrencyCode": currency_code,
                    "Amount": 100.00
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

@router.get("/get-report")
def get_report():
    params = {
        "reportTypes": ["GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA"]
    }
    return spapi_request("GET", "/reports/2021-06-30/reports", params=params)

@router.get("/get-report-document")
def get_report_document():
    reportDocumentId = "amzn1.spdoc.1.4.eu.9a686765-5232-4bfd-9df3-036b1671eae9.TZD0NOIO5NKQS.2610"
    return spapi_request("GET", f"/reports/2021-06-30/documents/{reportDocumentId}", params={})

@router.get("/get-raw-financial")
# "405-5308958-7314741" 171-6810812-4681165
def get_raw_financial_events(order_id: str = "405-5308958-7314741"):
    return spapi_request("GET", f"/finances/v0/orders/{order_id}/financialEvents", params={})

@router.get("/test-reports/orders-3days-preview")
def test_orders_report_3days_preview(max_rows: int = 20, poll_seconds: int = 5, max_polls: int = 30):
    """
    One-shot test:
      1) createReport for last 3 days (orders by last update)
      2) poll getReport until DONE
      3) getReportDocument
      4) download + decompress (if gzip)
      5) return header + first N rows preview

    Fix included:
      Your spapi_request() wrapper sometimes returns createReport response as:
        {"reportId": "..."}
      instead of:
        {"payload": {"reportId": "..."}}
      So this endpoint now supports BOTH shapes.
    """
    marketplace_id = os.getenv("MARKETPLACE_ID")
    if not marketplace_id:
        return {"error": "MARKETPLACE_ID env var not set"}

    report_type = "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL"

    end_time = datetime.utcnow().replace(microsecond=0)
    start_time = (end_time - timedelta(days=3)).replace(microsecond=0)

    # 1) Create report
    create_body = {
        "reportType": report_type,
        "dataStartTime": start_time.isoformat() + "Z",
        "dataEndTime": end_time.isoformat() + "Z",
        "marketplaceIds": [marketplace_id],
    }
    create_resp = spapi_request("POST", "/reports/2021-06-30/reports", body=create_body)

    if "errors" in create_resp:
        return {"stage": "createReport", "errors": create_resp.get("errors"), "body": create_body, "raw": create_resp}

    # Support BOTH response shapes:
    # - {"payload": {"reportId": "..."}}
    # - {"reportId": "..."}
    report_id = (create_resp.get("payload") or {}).get("reportId") or create_resp.get("reportId")
    if not report_id:
        return {"stage": "createReport", "error": "No reportId returned", "raw": create_resp}

    # 2) Poll report status
    report_doc_id = None
    status = None
    poll_history = []

    for _ in range(max_polls):
        get_resp = spapi_request("GET", f"/reports/2021-06-30/reports/{report_id}", params={})
        if "errors" in get_resp:
            return {"stage": "getReport", "reportId": report_id, "errors": get_resp.get("errors"), "raw": get_resp}

        payload = get_resp.get("payload") or get_resp  # tolerate weird wrapper shapes
        status = payload.get("processingStatus")
        poll_history.append({"processingStatus": status, "time": datetime.utcnow().isoformat() + "Z"})

        if status == "DONE":
            report_doc_id = payload.get("reportDocumentId")
            break
        if status in ("FATAL", "CANCELLED"):
            return {"stage": "getReport", "reportId": report_id, "processingStatus": status, "raw": payload}

        time.sleep(poll_seconds)

    if not report_doc_id:
        return {
            "stage": "polling",
            "reportId": report_id,
            "processingStatus": status,
            "polls": len(poll_history),
            "poll_history": poll_history,
            "error": "Report not DONE yet. Increase max_polls or poll_seconds and try again.",
        }

    # 3) Get report document (download URL)
    doc_resp = spapi_request("GET", f"/reports/2021-06-30/documents/{report_doc_id}", params={})
    if "errors" in doc_resp:
        return {
            "stage": "getReportDocument",
            "reportDocumentId": report_doc_id,
            "errors": doc_resp.get("errors"),
            "raw": doc_resp,
        }

    doc_payload = doc_resp.get("payload") or doc_resp  # tolerate weird wrapper shapes
    url = doc_payload.get("url")
    compression = (doc_payload.get("compressionAlgorithm") or "").upper() or None

    if not url:
        return {
            "stage": "getReportDocument",
            "reportDocumentId": report_doc_id,
            "error": "No url returned",
            "raw": doc_payload,
        }

    # 4) Download report file from presigned URL
    r = requests.get(url, timeout=60)
    r.raise_for_status()
    data = r.content

    # 5) Decompress if GZIP
    if compression == "GZIP":
        data = gzip.decompress(data)

    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()

    if not lines:
        return {
            "reportType": report_type,
            "dataStartTime": create_body["dataStartTime"],
            "dataEndTime": create_body["dataEndTime"],
            "marketplaceIds": create_body["marketplaceIds"],
            "reportId": report_id,
            "reportDocumentId": report_doc_id,
            "processingStatus": "DONE",
            "compressionAlgorithm": compression,
            "note": "Report downloaded but contains no lines.",
        }

    # Amazon flat-file order reports are typically TAB-delimited
    delimiter = "\t"
    reader = csv.reader(lines, delimiter=delimiter)
    header = next(reader, [])
    rows = []
    for i, row in enumerate(reader):
        if i >= max_rows:
            break
        rows.append(dict(zip(header, row)) if header and len(header) == len(row) else row)

    return {
        "reportType": report_type,
        "dataStartTime": create_body["dataStartTime"],
        "dataEndTime": create_body["dataEndTime"],
        "marketplaceIds": create_body["marketplaceIds"],
        "reportId": report_id,
        "reportDocumentId": report_doc_id,
        "processingStatus": "DONE",
        "compressionAlgorithm": compression,
        "delimiter": "TAB",
        "header_columns_count": len(header),
        "header_preview": header[:60],
        "rows_returned": len(rows),
        "rows_preview": rows,
        "raw_text_preview_first_10_lines": lines[:10],
    }