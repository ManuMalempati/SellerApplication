import csv
import gzip
import io
import time
import requests
from datetime import datetime, timedelta, timezone
from app.auth import spapi_request
from app.utilities.utils import retry_call
from config import MARKETPLACE_ID


def fetch_spapi_report(
    report_type: str,
    start_dt=None,
    end_dt=None,
    days: int = 365,
    output_type: str = "tsv",   # "raw", "tsv", "json"
    params: dict = None
):
    """
    Unified SP-API report fetcher.

    output_type:
        - "raw"  → return raw decoded text
        - "tsv"  → parse TSV into list[dict]
        - "json" → parse JSON into Python object
    """

    # Compute window if not explicitly provided
    if start_dt is None or end_dt is None:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(days=days)

    print(f"[SPAPI] Requesting {report_type} for {start_dt.isoformat()} -> {end_dt.isoformat()}")

    # Build request body
    body = {
        "reportType": report_type,
        "dataStartTime": start_dt.isoformat(),
        "dataEndTime": end_dt.isoformat(),
        "marketplaceIds": [MARKETPLACE_ID],
    }

    if params:
        body.update(params)

    # 1. Create report
    create_resp = retry_call(
        spapi_request,
        "POST",
        "/reports/2021-06-30/reports",
        body=body
    ) or {}

    if "reportId" not in create_resp:
        raise RuntimeError(f"Failed to create report: {create_resp}")

    report_id = create_resp["reportId"]
    print(f"[SPAPI] Report requested: {report_id}")

    # 2. Poll
    for _ in range(60):
        status_resp = retry_call(
            spapi_request,
            "GET",
            f"/reports/2021-06-30/reports/{report_id}"
        ) or {}

        status = status_resp.get("processingStatus")
        print(f"[SPAPI] Polling status: {status}")

        if status in ("DONE", "DONE_NO_DATA"):
            break

        time.sleep(5)
    else:
        raise RuntimeError(f"Timeout waiting for report {report_type}")

    # 3. Get document ID
    document_id = status_resp.get("reportDocumentId")
    if not document_id:
        raise RuntimeError(f"No reportDocumentId: {status_resp}")

    print(f"[SPAPI] Document ready: {document_id}")

    # 4. Get download URL
    doc_resp = retry_call(
        spapi_request,
        "GET",
        f"/reports/2021-06-30/documents/{document_id}"
    ) or {}

    if "url" not in doc_resp:
        raise RuntimeError(f"Failed to get document URL: {doc_resp}")

    url = doc_resp["url"]
    compression = doc_resp.get("compressionAlgorithm")

    print(f"[SPAPI] Downloading document...")
    raw = requests.get(url).content

    # 5. Decompress
    if compression == "GZIP":
        raw = gzip.decompress(raw)

    text = raw.decode("utf-8", errors="replace")

    # 6. Output modes
    if output_type == "raw":
        return text

    if output_type == "json":
        import json
        return json.loads(text)

    if output_type == "tsv":
        reader = csv.DictReader(io.StringIO(text), delimiter="\t")
        rows = list(reader)
        print(f"[SPAPI] Parsed {len(rows)} rows for {report_type}")
        return rows

    raise ValueError(f"Invalid output_type: {output_type}")