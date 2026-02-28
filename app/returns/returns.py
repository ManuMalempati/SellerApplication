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
from ..utils import clean_str, safe_int, safe_dt, now_utc_plus_offset_naive

load_dotenv()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")


# ---------------------------------------------------------
# Fetch FBA Customer Returns Report
# ---------------------------------------------------------

def fetch_fba_customer_returns(days=365):
    end_dt = datetime.now(timezone.utc)
    start_dt = end_dt - timedelta(days=days)

    print(f"[FBA-RETURNS] Requesting report for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    create_resp = spapi_request(
        method="POST",
        path="/reports/2021-06-30/reports",
        body={
            "reportType": "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA",
            "dataStartTime": start_dt.isoformat(),
            "dataEndTime": end_dt.isoformat(),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    if not create_resp or "reportId" not in create_resp:
        raise RuntimeError(f"Failed to create report: {create_resp}")

    report_id = create_resp["reportId"]
    print(f"[FBA-RETURNS] Report requested: {report_id}")

    # Poll until DONE
    for _ in range(60):
        status_resp = spapi_request(
            method="GET",
            path=f"/reports/2021-06-30/reports/{report_id}",
        )
        if status_resp and status_resp.get("processingStatus") == "DONE":
            break
        time.sleep(5)
    else:
        raise RuntimeError("Timeout waiting for FBA Customer Returns report")

    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"No reportDocumentId: {status_resp}")

    print(f"[FBA-RETURNS] Report document ready: {document_id}")

    # Get download URL
    doc_resp = spapi_request(
        method="GET",
        path=f"/reports/2021-06-30/documents/{document_id}"
    )
    if not doc_resp or "url" not in doc_resp:
        raise RuntimeError(f"Failed to get document URL: {doc_resp}")

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print("[FBA-RETURNS] Downloading document...")

    raw = requests.get(url).content

    if compression == "GZIP":
        decoded = gzip.decompress(raw).decode("utf-8", errors="replace")
    else:
        decoded = raw.decode("utf-8", errors="replace")

    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    print(f"[FBA-RETURNS] Parsed {len(rows)} rows")

    return rows


# ---------------------------------------------------------
# UPSERT + AGGREGATE + UPDATE ORDERITEMS
# ---------------------------------------------------------

def upsert_fba_customer_returns(rows):
    if not rows:
        print("[FBA-RETURNS] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    try:
        cursor.fast_executemany = True
    except:
        pass

    staging = []
    for r in rows:
        staging.append((
            safe_dt(r.get("return-date")),
            clean_str(r.get("order-id")),
            clean_str(r.get("sku")),
            clean_str(r.get("asin")),
            clean_str(r.get("fnsku")),
            clean_str(r.get("product-name")),
            safe_int(r.get("quantity")),
            clean_str(r.get("fulfillment-center-id")),
            clean_str(r.get("detailed-disposition")),
            clean_str(r.get("reason")),
            clean_str(r.get("license-plate-number")),
            clean_str(r.get("customer-comments")),
            now_utc_plus_offset_naive(),
            now_utc_plus_offset_naive(),
        ))

    # Create temp table
    cursor.execute("""
        IF OBJECT_ID('tempdb..#TempReturns') IS NOT NULL DROP TABLE #TempReturns;
        CREATE TABLE #TempReturns (
            return_date DATETIME,
            order_id NVARCHAR(50),
            sku NVARCHAR(200),
            asin NVARCHAR(20),
            fnsku NVARCHAR(50),
            product_name NVARCHAR(1000),
            quantity INT,
            fulfillment_center_id NVARCHAR(50),
            detailed_disposition NVARCHAR(200),
            reason NVARCHAR(500),
            license_plate_number NVARCHAR(200),
            customer_comments NVARCHAR(MAX),
            created_at DATETIME,
            updated_at DATETIME
        );
    """)

    cursor.executemany("""
        INSERT INTO #TempReturns VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
    """, staging)

    # MERGE UPSERT
    cursor.execute("""
        MERGE INTO spapi_app_user.FBACustomerReturns AS target
        USING #TempReturns AS src
        ON target.order_id = src.order_id
        AND target.sku = src.sku
        AND target.asin = src.asin
        AND target.fnsku = src.fnsku
        AND target.return_date = src.return_date
        AND target.license_plate_number = src.license_plate_number

        WHEN MATCHED THEN
            UPDATE SET
                target.product_name = src.product_name,
                target.quantity = src.quantity,
                target.fulfillment_center_id = src.fulfillment_center_id,
                target.detailed_disposition = src.detailed_disposition,
                target.reason = src.reason,
                target.customer_comments = src.customer_comments,
                target.updated_at = src.updated_at

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                return_date, order_id, sku, asin, fnsku, license_plate_number,
                product_name, quantity, fulfillment_center_id, detailed_disposition,
                reason, customer_comments, created_at, updated_at
            )
            VALUES (
                src.return_date, src.order_id, src.sku, src.asin, src.fnsku, src.license_plate_number,
                src.product_name, src.quantity, src.fulfillment_center_id, src.detailed_disposition,
                src.reason, src.customer_comments, src.created_at, src.updated_at
            );
    """)

    # ---------------------------------------------------------
    # AGGREGATE RETURNS → ONE ROW PER ORDERITEMS
    # ---------------------------------------------------------
    cursor.execute("""
        IF OBJECT_ID('tempdb..#AggReturns') IS NOT NULL DROP TABLE #AggReturns;

        SELECT
            order_id,
            sku,
            SUM(quantity) AS ReturnQty,
            MAX(return_date) AS ReturnDate,
            MAX(detailed_disposition) AS ReturnDisposition,
            MAX(reason) AS ReturnReason,
            MAX(license_plate_number) AS LicensePlateNumber
        INTO #AggReturns
        FROM spapi_app_user.FBACustomerReturns
        GROUP BY order_id, sku;
    """)

    # ---------------------------------------------------------
    # UPDATE ORDERITEMS WITH CONSOLIDATED RETURN INFO
    # ---------------------------------------------------------
    cursor.execute("""
        UPDATE O
        SET 
            O.ReturnQty          = A.ReturnQty,
            O.ReturnDate         = A.ReturnDate,
            O.ReturnDisposition  = A.ReturnDisposition,
            O.ReturnReason       = A.ReturnReason,
            O.LicensePlateNumber = A.LicensePlateNumber
        FROM OrderItems O
        JOIN #AggReturns A
          ON O.AmazonOrderId = A.order_id
         AND O.SKU           = A.sku;
    """)

    conn.commit()
    cursor.close()
    conn.close()

    print("[FBA-RETURNS] Upsert + Aggregation complete")
    return True


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_returns_import(days):
    print("==============================================")
    print("FBA CUSTOMER RETURNS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_customer_returns(days)
    upsert_fba_customer_returns(rows)

    print("==============================================")
    print("FBA CUSTOMER RETURNS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":
    run_returns_import(days=365)