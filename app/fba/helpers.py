import time
import threading
import requests
import gzip

from app.utils import retry_call
from app.auth import spapi_request
import config

progress_lock = threading.Lock()
pricing_progress = {"done": 0, "total": 0}
fees_progress = {"done": 0, "total": 0}

def throttle():
    time.sleep(0.5)

def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]

# ---------------------------------------------------------
# Request Report
# ---------------------------------------------------------
def request_report(report_type, params=None):
    throttle()
    print(f"Requesting report: {report_type}")

    body = {"reportType": report_type, "marketplaceIds": [config.MARKETPLACE_ID]}
    if params:
        body.update(params)

    resp = retry_call(spapi_request, "POST", "/reports/2021-06-30/reports", body=body) or {}
    print("Report request response:", resp)

    return resp.get("reportId")

# ---------------------------------------------------------
# Wait for Report
# ---------------------------------------------------------
def wait_for_report(report_id, timeout=300):
    if not report_id:
        raise ValueError("wait_for_report called with empty report_id")

    print(f"Waiting for report {report_id}...")
    start = time.time()

    while True:
        throttle()
        resp = retry_call(spapi_request, "GET", f"/reports/2021-06-30/reports/{report_id}") or {}
        status = resp.get("processingStatus") or "UNKNOWN"
        print(f"   -> Status: {status}")

        if status == "DONE":
            doc_id = resp.get("reportDocumentId")
            if not doc_id:
                raise ValueError("Report DONE but no reportDocumentId in response")
            return doc_id

        if time.time() - start > timeout:
            raise TimeoutError("Report timed out")

        time.sleep(2)

# ---------------------------------------------------------
# Download Report
# ---------------------------------------------------------
def download_report(document_id, is_json=False):
    if not document_id:
        raise ValueError("download_report called with empty document_id")

    print(f"Downloading report document {document_id}...")
    throttle()

    doc = retry_call(spapi_request, "GET", f"/reports/2021-06-30/documents/{document_id}") or {}
    url = doc.get("url")
    if not url:
        raise ValueError("No URL in report document response")

    raw = requests.get(url).content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)

    def safe_decode(b: bytes) -> str:
        try:
            return b.decode("utf-8")
        except UnicodeDecodeError:
            return b.decode("cp1252", errors="replace")

    if is_json:
        import json
        return json.loads(safe_decode(raw))

    return safe_decode(raw)