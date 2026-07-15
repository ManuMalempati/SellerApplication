import time
from datetime import datetime

from app.utilities.fetch_report import fetch_spapi_report


def safe_int(value):
    try:
        if value in (None, "", " "):
            return None

        return int(value)

    except Exception:
        return None


async def inventory_ledger_detail_report(
    start_time=None,
    end_time=None
):
    """
    Fetches and normalizes:

        GET_LEDGER_DETAIL_VIEW_DATA

    Optional:
        start_time
        end_time

    Example:
        2026-01-01T00:00:00Z
    """

    started_at = time.time()

    print("Requesting Inventory Ledger Detail report...")
    print(f"Start Time: {start_time}")
    print(f"End Time:   {end_time}")

    # ---------------------------------------------------------
    # Convert ISO strings into datetime objects expected by
    # fetch_spapi_report()
    # ---------------------------------------------------------
    start_dt = None
    end_dt = None

    if start_time:
        start_dt = datetime.fromisoformat(
            start_time.replace("Z", "+00:00")
        )

    if end_time:
        end_dt = datetime.fromisoformat(
            end_time.replace("Z", "+00:00")
        )

    rows_tsv = fetch_spapi_report(
        report_type="GET_LEDGER_DETAIL_VIEW_DATA",
        output_type="tsv",
        start_dt=start_dt,
        end_dt=end_dt
    )

    print(f"Fetched {len(rows_tsv)} raw rows")

    rows = []

    for line in rows_tsv:

        # ---------------------------------------------------------
        # Parse Date
        # Example:
        # 07/13/2026
        # ---------------------------------------------------------
        ledger_date = None

        try:
            raw_date = line.get("Date")

            if raw_date:
                ledger_date = datetime.strptime(
                    raw_date,
                    "%m/%d/%Y"
                ).date()

        except Exception:
            pass

        # ---------------------------------------------------------
        # Parse Date and Time
        # Example:
        # 2026-07-13T01:00:00+0100
        # ---------------------------------------------------------
        ledger_datetime = None

        try:
            raw_datetime = line.get("Date and Time")

            if raw_datetime:
                ledger_datetime = datetime.strptime(
                    raw_datetime,
                    "%Y-%m-%dT%H:%M:%S%z"
                ).replace(
                    tzinfo=None
                )

        except Exception:
            pass

        rows.append({
            "Date": ledger_date,
            "DateTime": ledger_datetime,

            "FNSKU": line.get("FNSKU"),
            "ASIN": line.get("ASIN"),
            "SKU": line.get("MSKU"),

            "Title": line.get("Title"),

            "EventType": line.get("Event Type"),
            "ReferenceID": line.get("Reference ID"),

            "Quantity": safe_int(
                line.get("Quantity")
            ),

            "FulfillmentCenter": line.get(
                "Fulfillment Center"
            ),

            "Disposition": line.get(
                "Disposition"
            ),

            "Reason": line.get(
                "Reason"
            ),

            "Country": line.get(
                "Country"
            ),

            "ReconciledQuantity": safe_int(
                line.get("Reconciled Quantity")
            ),

            "UnreconciledQuantity": safe_int(
                line.get("Unreconciled Quantity")
            ),
        })

    elapsed = time.time() - started_at

    print("=" * 60)
    print("INVENTORY LEDGER DETAIL REPORT")
    print("=" * 60)
    print(f"Rows: {len(rows):,}")
    print(f"Time: {elapsed:.1f}s")
    print("=" * 60)

    return rows