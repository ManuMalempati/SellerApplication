"""
Inventory Ledger - Adjustment Ingestion

Purpose
-------
Fetches Amazon Inventory Ledger Detail data and returns
normalized adjustment events.

Important Findings
------------------
1. We only ingest EventType = 'Adjustments'.

   Other event types such as:
       - Receipts
       - Shipments
       - WhseTransfers
       - CustomerReturns

   are not currently required for the inventory
   reimbursement workflow.

2. Amazon adjustment rows contain a Reference ID.

   Investigation of historical data showed:

       (ReferenceID)

   is unique for adjustment events.

   This makes ReferenceID the natural business key for
   adjustment ingestion and idempotency.

3. ReconciledQuantity and UnreconciledQuantity are stored
   but should NOT currently be used as the source of truth
   for event reconciliation.

   Investigation found examples where:

       Inventory misplaced (M)

   appeared chronologically AFTER a corresponding
   Inventory found (F) adjustment while still being marked
   as fully reconciled.

   These fields appear useful as Amazon reconciliation
   metadata but are not currently reliable enough to drive
   business logic.

4. Lost / damaged identification should be based on
   actual adjustment events:

       M
       5
       6
       7
       E
       H
       K
       U

   and resolution events:

       F
       N

5. Date values are converted to native Python date /
   datetime objects before insertion to avoid SQL Server
   conversion issues.

Report
------
GET_LEDGER_DETAIL_VIEW_DATA
"""

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
    Fetch and normalize Inventory Ledger adjustments.

    Parameters
    ----------
    start_time : str
        ISO datetime string:
        2026-01-01T00:00:00Z

    end_time : str
        ISO datetime string

    Returns
    -------
    list[dict]

    Only adjustment events are returned.
    """

    started_at = time.time()

    print("Requesting Inventory Ledger Detail report...")
    print(f"Start Time: {start_time}")
    print(f"End Time:   {end_time}")

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

        # Only adjustments matter for the reconciliation
        # workflow.
        if line.get("Event Type") != "Adjustments":
            continue

        ledger_date = None
        ledger_datetime = None

        try:
            raw_date = line.get("Date")

            if raw_date:
                ledger_date = datetime.strptime(
                    raw_date,
                    "%m/%d/%Y"
                ).date()

        except Exception:
            pass

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
    print(f"Adjustment Rows: {len(rows):,}")
    print(f"Time: {elapsed:.1f}s")
    print("=" * 60)

    return rows