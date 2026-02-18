import time
import threading
import requests
import gzip

from ..auth import spapi_request
from .config import MARKETPLACE_ID, MAX_RETRIES, INITIAL_RETRY_DELAY, SELLER_ID, MAX_WORKERS

progress_lock = threading.Lock()
pricing_progress = {"done": 0, "total": 0}
fees_progress = {"done": 0, "total": 0}


def throttle():
    time.sleep(0.5)


def chunk(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def retry_api_call(func, *args, max_retries=MAX_RETRIES, initial_delay=INITIAL_RETRY_DELAY, **kwargs):
    delay = initial_delay
    resp = None
    for attempt in range(max_retries):
        resp = func(*args, **kwargs)
        if isinstance(resp, dict) and "errors" in resp:
            codes = [e.get("code") for e in resp["errors"]]
            if "QuotaExceeded" in codes or "RequestThrottled" in codes:
                if attempt < max_retries - 1:
                    print(f"[RETRY] Throttled {codes}, retry {attempt+1}/{max_retries} in {delay}s")
                    time.sleep(delay)
                    delay *= 2
                    continue
        return resp
    return resp


def request_report(report_type, params=None):
    throttle()
    print(f"Requesting report: {report_type}")
    body = {"reportType": report_type, "marketplaceIds": [MARKETPLACE_ID]}
    if params:
        body.update(params)

    resp = spapi_request("POST", "/reports/2021-06-30/reports", body=body) or {}
    print("Report request response:", resp)

    report_id = resp.get("reportId")
    if not report_id:
        print("No reportId in response, aborting.")
        return None
    return report_id


def wait_for_report(report_id, timeout=300):
    if not report_id:
        raise ValueError("wait_for_report called with empty report_id")

    print(f"Waiting for report {report_id}...")
    start = time.time()

    while True:
        throttle()
        resp = spapi_request("GET", f"/reports/2021-06-30/reports/{report_id}") or {}
        status = resp.get("processingStatus") or "UNKNOWN"
        print(f"   -> Status: {status}")

        if status == "DONE":
            print("Report is DONE")
            doc_id = resp.get("reportDocumentId")
            if not doc_id:
                raise ValueError("Report DONE but no reportDocumentId in response")
            return doc_id

        if time.time() - start > timeout:
            raise TimeoutError("Report timed out")

        time.sleep(2)


def download_report(document_id, is_json=False):
    if not document_id:
        raise ValueError("download_report called with empty document_id")

    print(f"Downloading report document {document_id}...")
    throttle()

    doc = spapi_request("GET", f"/reports/2021-06-30/documents/{document_id}") or {}
    url = doc.get("url")
    if not url:
        raise ValueError("No URL in report document response")

    raw = requests.get(url).content
    if doc.get("compressionAlgorithm") == "GZIP":
        raw = gzip.decompress(raw)

    print("Report downloaded.")
    
    if is_json:
        import json
        return json.loads(raw.decode("utf-8"))
    
    return raw.decode("utf-8")
