# RESPONSIBLE FOR FBARemovalOrders Table

import csv
import gzip
import io
import time
from datetime import datetime, timedelta, timezone
import requests
from app.database import connect_database
from app.auth import spapi_request
from app.utils import clean_str, safe_int, safe_float, safe_dt, now_utc_plus_offset_naive
from config import MARKETPLACE_ID


# ---------------------------------------------------------
# Fetch FBA Removal Order Detail Report
# ---------------------------------------------------------

def fetch_fba_removal_orders(days=365):

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    print(f"[FBA-REMOVAL] Requesting report for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        raise RuntimeError(f"Failed to create report: {create_resp}")

    report_id = create_resp["reportId"]
    print(f"[FBA-REMOVAL] Report requested: {report_id}")

    # Poll
    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )
        status = status_resp.get("processingStatus")
        print(f"[FBA-REMOVAL] Polling status: {status}")

        if status in ("DONE", "DONE_NO_DATA"):
            break

        time.sleep(5)
    else:
        raise RuntimeError("Timeout waiting for FBA Removal Order Detail report")

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"No reportDocumentId: {status_resp}")

    print(f"[FBA-REMOVAL] Report document ready: {document_id}")

    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )

    if not doc_resp or "url" not in doc_resp:
        raise RuntimeError(f"Failed to get document URL: {doc_resp}")

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print("[FBA-REMOVAL] Downloading document...")
    raw = requests.get(url).content

    if compression == "GZIP":
        decoded = gzip.decompress(raw).decode("utf-8", errors="replace")
    else:
        decoded = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    print(f"[FBA-REMOVAL] Parsed {len(rows)} rows")
    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT
# ---------------------------------------------------------

def upsert_fba_removal_orders(rows):

    if not rows:
        print("[FBA-REMOVAL] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    order_ids = sorted({
        clean_str(r.get("order-id"))
        for r in rows
        if clean_str(r.get("order-id"))
    })

    print(f"[FBA-REMOVAL] Deleting existing rows for {len(order_ids)} order-ids")

    if order_ids:
        cursor.execute(
            "DELETE FROM spapi_app_user.FBARemovalOrders WHERE order_id IN (%s)" %
            ",".join("?" for _ in order_ids),
            order_ids
        )
        conn.commit()

    print("[FBA-REMOVAL] Inserting fresh rows...")

    staging = []
    for r in rows:
        staging.append((
            clean_str(r.get("order-id")),
            clean_str(r.get("sku")),
            clean_str(r.get("disposition")),
            safe_dt(r.get("request-date")),
            clean_str(r.get("order-type")),
            clean_str(r.get("service-speed")),
            clean_str(r.get("order-status")),
            safe_dt(r.get("last-updated-date")),
            clean_str(r.get("fnsku")),
            safe_int(r.get("requested-quantity")),
            safe_int(r.get("cancelled-quantity")),
            safe_int(r.get("disposed-quantity")),
            safe_int(r.get("shipped-quantity")),
            safe_int(r.get("in-process-quantity")),
            safe_float(r.get("removal-fee")),
            clean_str(r.get("currency")),
            now_utc_plus_offset_naive(),   # created_at
            now_utc_plus_offset_naive(),   # updated_at
        ))

    cursor.fast_executemany = True

    cursor.executemany("""
        INSERT INTO spapi_app_user.FBARemovalOrders (
            order_id,
            sku,
            disposition,
            request_date,
            order_type,
            service_speed,
            order_status,
            last_updated_date,
            fnsku,
            requested_quantity,
            cancelled_quantity,
            disposed_quantity,
            shipped_quantity,
            in_process_quantity,
            removal_fee,
            currency,
            created_at,
            updated_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, staging)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FBA-REMOVAL] Inserted {len(staging)} rows")
    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_removal_orders_import(days=365):
    print("==============================================")
    print("FBA REMOVAL ORDERS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_removal_orders(days)
    upsert_fba_removal_orders(rows)

    print("==============================================")
    print("FBA REMOVAL ORDERS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":

    run_removal_orders_import(days=365)