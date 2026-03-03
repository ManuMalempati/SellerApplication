import time
from datetime import datetime, timedelta
from app.utilities.fetch_report import fetch_spapi_report   # <-- unified fetcher
from config import MARKETPLACE_ID


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

    print(f"[L30] Requesting Sales & Traffic: {dataStartTime} -> {dataEndTime}")

    # ---------------------------------------------------------
    # Use unified fetcher (JSON mode)
    # ---------------------------------------------------------
    data = fetch_spapi_report(
        report_type="GET_SALES_AND_TRAFFIC_REPORT",
        output_type="json",
        params={
            "reportOptions": {
                "dateGranularity": "DAY",
                "asinGranularity": "CHILD"
            },
            "dataStartTime": dataStartTime,
            "dataEndTime": dataEndTime,
            "marketplaceIds": [MARKETPLACE_ID]
        }
    )

    # ---------------------------------------------------------
    # Extract L-30 totals per ASIN
    # ---------------------------------------------------------
    asin_rows = data.get("salesAndTrafficByAsin", [])
    results = {}

    for row in asin_rows:
        asin = row.get("parentAsin") or row.get("childAsin")
        if not asin:
            continue

        sales = row.get("salesByAsin", {})
        traffic = row.get("trafficByAsin", {})

        # orderedProductSales may be dict {amount, currencyCode}
        ordered_sales = sales.get("orderedProductSales", 0)
        if isinstance(ordered_sales, dict):
            ordered_sales = ordered_sales.get("amount", 0)

        results[asin] = {
            "TotalOrderItems_L30": sales.get("totalOrderItems", 0),
            "OrderedProductSales_L30": ordered_sales,
            "UnitsRefunded_L30": sales.get("unitsRefunded", 0),
            "BuyBoxPercentage_L30": traffic.get("buyBoxPercentage", 0),
        }

    print(f"[L30] Extracted L-30 data for {len(results)} ASINs")
    return results