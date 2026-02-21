#!/usr/bin/env python3
import csv
import gzip
import io
import time
from datetime import datetime, timedelta, timezone

import os
from dotenv import load_dotenv
import requests

from ..database import connect_database
from ..auth import spapi_request

load_dotenv()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")


# ---------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------
def clean_str(x):
    if x is None:
        return None
    x = str(x).strip()
    return x if x != "" else None


def safe_int(x):
    try:
        x = str(x).strip()
        if x in ("", " ", "-", "--", "N/A", "NA", "None", "null"):
            return 0
        return int(float(x))
    except:
        return 0


def safe_dt(x):
    if not x:
        return None

    x = str(x).strip()

    if x in ("", " ", "N/A", "NA", "-", "--", "0000-00-00T00:00:00+00:00"):
        return None

    try:
        if x.endswith("Z"):
            x = x.replace("Z", "+00:00")

        return datetime.fromisoformat(x)

    except:
        return None


# ---------------------------------------------------------
# Fetch FBA Removal Shipment Detail Report
# ---------------------------------------------------------
def fetch_fba_removal_shipments(days):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    print(f"[FBA-REM-SHIP] Requesting report for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        raise RuntimeError(f"Failed to create report: {create_resp}")

    report_id = create_resp["reportId"]
    print(f"[FBA-REM-SHIP] Report requested: {report_id}")

    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )

        status = status_resp.get("processingStatus")
        print(f"[FBA-REM-SHIP] Polling status: {status}")

        if status in ("DONE", "DONE_NO_DATA"):
            break

        time.sleep(5)
    else:
        raise RuntimeError("Timeout waiting for FBA Removal Shipment Detail report")

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"No reportDocumentId: {status_resp}")

    print(f"[FBA-REM-SHIP] Report document ready: {document_id}")

    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )

    if not doc_resp or "url" not in doc_resp:
        raise RuntimeError(f"Failed to get document URL: {doc_resp}")

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print("[FBA-REM-SHIP] Downloading document...")

    raw = requests.get(url).content

    if compression == "GZIP":
        decoded = gzip.decompress(raw).decode("utf-8", errors="replace")
    else:
        decoded = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    print(f"[FBA-REM-SHIP] Parsed {len(rows)} rows")

    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT
# ---------------------------------------------------------
def upsert_fba_removal_shipments(rows):

    if not rows:
        print("[FBA-REM-SHIP] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    order_ids = sorted({
        clean_str(r.get("order-id"))
        for r in rows
        if clean_str(r.get("order-id"))
    })

    print(f"[FBA-REM-SHIP] Deleting existing rows for {len(order_ids)} order-ids")

    if order_ids:
        cursor.execute(
            "DELETE FROM spapi_app_user.FBARemovalShipments WHERE order_id IN (%s)" %
            ",".join("?" for _ in order_ids),
            order_ids
        )
        conn.commit()

    print("[FBA-REM-SHIP] Inserting fresh rows...")

    staging = []

    for r in rows:

        staging.append((
            clean_str(r.get("order-id")),
            clean_str(r.get("sku")),
            clean_str(r.get("disposition")),
            clean_str(r.get("tracking-number")),
            safe_dt(r.get("request-date")),
            safe_dt(r.get("shipment-date")),
            clean_str(r.get("fnsku")),
            safe_int(r.get("shipped-quantity")),
            clean_str(r.get("carrier")),
            clean_str(r.get("removal-order-type")),
        ))

    cursor.fast_executemany = True

    cursor.executemany("""

        INSERT INTO spapi_app_user.FBARemovalShipments (

            order_id,
            sku,
            disposition,
            tracking_number,
            request_date,
            shipment_date,
            fnsku,
            shipped_quantity,
            carrier,
            removal_order_type

        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)

    """, staging)

    conn.commit()

    cursor.close()
    conn.close()

    print(f"[FBA-REM-SHIP] Inserted {len(staging)} rows")

    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------
def run_removal_shipments_import(days):

    print("==============================================")
    print("FBA REMOVAL SHIPMENTS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_removal_shipments(days)

    upsert_fba_removal_shipments(rows)

    print("==============================================")
    print("FBA REMOVAL SHIPMENTS IMPORT - COMPLETE")
    print("==============================================")


if __name__ == "__main__":
    run_removal_shipments_import(days=365)