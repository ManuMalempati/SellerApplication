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
from ..utils import clean_str, safe_int, safe_float, safe_dt, now_utc_plus_offset_naive

load_dotenv()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")


# ---------------------------------------------------------
# Fetch FBA Reimbursements Report
# ---------------------------------------------------------

def fetch_fba_reimbursements(days=365):

    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    print(f"[FBA-REIMB] Requesting report for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FBA_REIMBURSEMENTS_DATA",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        raise RuntimeError(f"Failed to create report: {create_resp}")

    report_id = create_resp["reportId"]
    print(f"[FBA-REIMB] Report requested: {report_id}")

    # Poll
    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )
        status = status_resp.get("processingStatus")
        print(f"[FBA-REIMB] Polling status: {status}")

        if status in ("DONE", "DONE_NO_DATA"):
            break

        time.sleep(5)
    else:
        raise RuntimeError("Timeout waiting for FBA Reimbursements report")

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"No reportDocumentId: {status_resp}")

    print(f"[FBA-REIMB] Report document ready: {document_id}")

    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )

    if not doc_resp or "url" not in doc_resp:
        raise RuntimeError(f"Failed to get document URL: {doc_resp}")

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print("[FBA-REIMB] Downloading document...")
    raw = requests.get(url).content

    if compression == "GZIP":
        decoded = gzip.decompress(raw).decode("utf-8", errors="replace")
    else:
        decoded = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    print(f"[FBA-REIMB] Parsed {len(rows)} rows")
    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT + AGG → ORDERITEMS
# ---------------------------------------------------------

def upsert_fba_reimbursements(rows):

    if not rows:
        print("[FBA-REIMB] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    # 1. Delete existing rows for these reimbursement_ids
    reimb_ids = sorted({
        clean_str(r.get("reimbursement-id"))
        for r in rows
        if clean_str(r.get("reimbursement-id"))
    })

    print(f"[FBA-REIMB] Deleting existing rows for {len(reimb_ids)} reimbursement-ids")

    if reimb_ids:
        cursor.execute(
            "DELETE FROM spapi_app_user.FBAReimbursements WHERE reimbursement_id IN (%s)" %
            ",".join("?" for _ in reimb_ids),
            reimb_ids
        )
        conn.commit()

    print("[FBA-REIMB] Inserting fresh rows...")

    # 2. Insert raw rows
    staging = []
    for r in rows:
        staging.append((
            clean_str(r.get("reimbursement-id")),
            safe_dt(r.get("approval-date")),
            clean_str(r.get("case-id")),
            clean_str(r.get("amazon-order-id")),
            clean_str(r.get("reason")),
            clean_str(r.get("sku")),
            clean_str(r.get("fnsku")),
            clean_str(r.get("asin")),
            clean_str(r.get("product-name")),
            clean_str(r.get("condition")),
            clean_str(r.get("currency-unit")),
            safe_float(r.get("amount-per-unit")),
            safe_float(r.get("amount-total")),
            safe_int(r.get("quantity-reimbursed-cash")),
            safe_int(r.get("quantity-reimbursed-inventory")),
            safe_int(r.get("quantity-reimbursed-total")),
            clean_str(r.get("original-reimbursement-id")),
            clean_str(r.get("original-reimbursement-type")),
            now_utc_plus_offset_naive(),
            now_utc_plus_offset_naive(),
        ))

    cursor.fast_executemany = True

    cursor.executemany("""
        INSERT INTO spapi_app_user.FBAReimbursements (
            reimbursement_id,
            approval_date,
            case_id,
            amazon_order_id,
            reason,
            sku,
            fnsku,
            asin,
            product_name,
            condition,
            currency_unit,
            amount_per_unit,
            amount_total,
            quantity_reimbursed_cash,
            quantity_reimbursed_inventory,
            quantity_reimbursed_total,
            original_reimbursement_id,
            original_reimbursement_type,
            created_at,
            updated_at
        )
        VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, staging)

    # 3. Aggregate reimbursements at AmazonOrderId + SKU level
    cursor.execute("""
        IF OBJECT_ID('tempdb..#AggReimb') IS NOT NULL DROP TABLE #AggReimb;

        SELECT
            amazon_order_id,
            sku,
            SUM(amount_total) AS total_amount,
            SUM(quantity_reimbursed_cash) AS qty_cash,
            SUM(quantity_reimbursed_inventory) AS qty_inv,
            SUM(quantity_reimbursed_total) AS qty_total,
            MAX(approval_date) AS approval_date
        INTO #AggReimb
        FROM spapi_app_user.FBAReimbursements
        GROUP BY amazon_order_id, sku;
    """)

    # 4. Push aggregated reimbursement into OrderItems
    cursor.execute("""
        UPDATE O
        SET 
            O.Reimbursed = A.total_amount,
            O.ReimbDate  = A.approval_date
        FROM OrderItems O
        JOIN #AggReimb A
          ON O.AmazonOrderId = A.amazon_order_id
         AND O.SKU           = A.sku;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FBA-REIMB] Inserted {len(staging)} rows (raw) and updated OrderItems with aggregated reimbursements")
    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_reimbursements_import(days=365):
    print("==============================================")
    print("FBA REIMBURSEMENTS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_reimbursements(days)
    upsert_fba_reimbursements(rows)

    print("==============================================")
    print("FBA REIMBURSEMENTS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":
    run_reimbursements_import(days=365)