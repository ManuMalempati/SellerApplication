import time
from datetime import datetime, timedelta

from ..auth import spapi_request
from ...config import MARKETPLACE_ID
from .helpers import throttle, download_report


def fetch_l30_sales_traffic():
    """
    Fetch last 30 days sales & traffic data per ASIN.
    Returns dict: {asin: {TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30, BuyBoxPercentage_L30}}
    """
    
    # Build L-30 date range
    end = datetime.utcnow()
    start = end - timedelta(days=30)

    dataStartTime = start.strftime("%Y-%m-%dT00:00:00Z")
    dataEndTime = end.strftime("%Y-%m-%dT00:00:00Z")

    print(f"Requesting L-30 sales/traffic: {dataStartTime} -> {dataEndTime}")

    # Request the report
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
        print(f"No reportId for sales/traffic report: {resp}")
        return {}

    print(f"Sales/Traffic report requested: {report_id}")

    # Poll until DONE
    document_id = None
    start_time = time.time()
    timeout = 300

    while True:
        throttle()
        status_resp = spapi_request(
            "GET",
            f"/reports/2021-06-30/reports/{report_id}"
        ) or {}

        status = status_resp.get("processingStatus")
        print(f"Sales/Traffic status: {status}")

        if status == "DONE":
            document_id = status_resp.get("reportDocumentId")
            break

        if status in ("CANCELLED", "FATAL"):
            print(f"Sales/Traffic report failed: {status}")
            return {}

        if time.time() - start_time > timeout:
            print("Sales/Traffic report timed out")
            return {}

        time.sleep(2)

    if not document_id:
        print("No document_id for sales/traffic report")
        return {}

    print(f"Sales/Traffic report ready: {document_id}")

    # Download the JSON report
    data = download_report(document_id, is_json=True)

    # Extract L-30 totals per ASIN
    asin_rows = data.get("salesAndTrafficByAsin", [])
    results = {}

    for row in asin_rows:
        asin = row.get("parentAsin") or row.get("childAsin")
        if not asin:
            continue

        sales = row.get("salesByAsin", {})
        traffic = row.get("trafficByAsin", {})

        # Handle orderedProductSales which can be a dict with amount/currencyCode
        ordered_sales = sales.get("orderedProductSales", 0)
        if isinstance(ordered_sales, dict):
            ordered_sales = ordered_sales.get("amount", 0)

        results[asin] = {
            "TotalOrderItems_L30": sales.get("totalOrderItems", 0),
            "OrderedProductSales_L30": ordered_sales,
            "UnitsRefunded_L30": sales.get("unitsRefunded", 0),
            "BuyBoxPercentage_L30": traffic.get("buyBoxPercentage", 0),
        }

    print(f"Extracted L-30 data for {len(results)} ASINs")
    return results
