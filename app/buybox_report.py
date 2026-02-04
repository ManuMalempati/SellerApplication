import os
import time
import csv
from io import StringIO
import requests

from .auth import spapi_request
from .database import connect_database, get_all_product_mapping

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def throttle():
    """Avoid hammering SP-API."""
    time.sleep(0.5)


def request_report(report_type, params=None):
    """Request a report and return the reportId."""
    throttle()

    # REQUIRED: marketplaceIds must be included for POST
    body = {
        "reportType": report_type,
        "marketplaceIds": [MARKETPLACE_ID]
    }

    if params:
        body.update(params)

    resp = spapi_request(
        "POST",
        "/reports/2021-06-30/reports",
        body=body
    )

    report_id = resp.get("reportId")
    if not report_id:
        raise Exception(f"Failed to request report: {resp}")

    return report_id


def wait_for_report(report_id, timeout=300):
    """Poll until report is DONE."""
    start = time.time()

    while True:
        throttle()

        resp = spapi_request(
            "GET",
            f"/reports/2021-06-30/reports/{report_id}"
        )

        status = resp.get("processingStatus")

        if status == "DONE":
            return resp.get("reportDocumentId")

        if status in ("CANCELLED", "FATAL"):
            raise Exception(f"Report failed: {status}")

        if time.time() - start > timeout:
            raise TimeoutError("Report generation timed out")

        time.sleep(2)


def download_report(document_id):
    """Download and return the raw text of the report."""
    throttle()

    doc = spapi_request(
        "GET",
        f"/reports/2021-06-30/documents/{document_id}"
    )

    url = doc.get("url")
    compression = doc.get("compressionAlgorithm")

    r = requests.get(url)
    raw = r.content

    if compression == "GZIP":
        import gzip
        raw = gzip.decompress(raw)

    return raw.decode("utf-8")


# ---------------------------------------------------------
# MAIN FUNCTION — FBA Inventory Report
# ---------------------------------------------------------

def buyboxes():
    """
    Fetch FBA inventory using GET_AFN_INVENTORY_DATA report.
    Returns rows with:
        sku
        asin
        ssku
        fnsku
        FBA-Stock (only > 0)
    """

    # Load SKU → ASIN → SSKU mapping
    conn = connect_database()
    cursor = conn.cursor()
    product_mappings = get_all_product_mapping(cursor)
    cursor.close()
    conn.close()

    # -----------------------------------------------------
    # 1. Request the FBA Inventory Report
    # -----------------------------------------------------
    report_id = request_report("GET_AFN_INVENTORY_DATA")
    print(f"Requested report: {report_id}")

    # -----------------------------------------------------
    # 2. Wait for report to finish
    # -----------------------------------------------------
    document_id = wait_for_report(report_id)
    print(f"Report ready: documentId={document_id}")

    # -----------------------------------------------------
    # 3. Download the report
    # -----------------------------------------------------
    raw_text = download_report(document_id)

    # -----------------------------------------------------
    # 4. Parse the tab-delimited file
    # -----------------------------------------------------
    f = StringIO(raw_text)
    reader = csv.DictReader(f, delimiter="\t")

    rows = []

    for line in reader:
        sku = line.get("seller-sku")
        asin = line.get("asin")
        fnsku = line.get("fulfillment-channel-sku")
        qty = line.get("Quantity Available")

        if not sku:
            continue

        # Convert qty safely
        qty_int = int(qty) if qty and qty.isdigit() else 0

        # Only include SKUs with stock > 0
        if qty_int <= 0:
            continue

        # Map to SSKU
        mapping = product_mappings.get(sku, {})
        ssku = mapping.get("ssku")

        row = {
            "sku": sku,
            "asin": asin,
            "ssku": ssku,
            "fnsku": fnsku,
            "FBA-Stock": qty_int,
        }

        rows.append(row)

    return rows
