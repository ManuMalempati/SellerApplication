import time
import threading
import requests
import gzip

from ..auth import spapi_request
from .config import MARKETPLACE_ID, MAX_RETRIES, INITIAL_RETRY_DELAY, SELLER_ID, MAX_WORKERS
from .rate_limiter import listings_limiter

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


# ---------------- listings (GET /listings/2021-08-01/items/{sellerId}/{sku}) helpers ----------------

def _get_listing_api(sku):
    """
    Low-level call to Listings API for a single SKU. Acquires listings rate limiter.
    Returns raw response (dict) or {}.
    """
    if not SELLER_ID:
        # No seller id configured
        return {}
    listings_limiter.acquire()
    params = {"marketplaceIds": [MARKETPLACE_ID]} if MARKETPLACE_ID else {}
    return spapi_request("GET", f"/listings/2021-08-01/items/{SELLER_ID}/{sku}", params=params) or {}


def fetch_listing_title(sku):
    """
    Call the listings API (with retry) and extract itemName from summaries[0].itemName.
    Returns:
      - itemName string on success
      - None when not available / parsing failed
      - {"_quota": True} when response contained QuotaExceeded/RequestThrottled (so caller can back off)
    """
    if not sku:
        return None
    if not SELLER_ID:
        return None

    resp = retry_api_call(_get_listing_api, sku)
    if not isinstance(resp, dict):
        return None

    # If the API returned errors, surface quota/throttle so caller can back off
    if "errors" in resp:
        codes = [e.get("code") for e in resp.get("errors", []) if isinstance(e, dict)]
        if "QuotaExceeded" in codes or "RequestThrottled" in codes:
            return {"_quota": True}
        # non-quota errors -> treat as missing title
        return None

    summaries = resp.get("summaries") or []
    if summaries and isinstance(summaries, list):
        item_name = summaries[0].get("itemName")
        return item_name
    return None


def get_listings_titles(skus):
    """
    Concurrent, batched fetch of itemName for a list of SKUs.
    Strategy:
      - Process SKUs in batches to avoid bursting the API.
      - Use a small ThreadPoolExecutor per batch with max_workers limited by MAX_WORKERS.
      - If quota errors are observed in a batch, apply a growing backoff before the next batch.
    Returns dict: {sku: itemName_or_None}
    """
    if not skus:
        return {}
    if not SELLER_ID:
        print("[listings] SELLER_ID not set; skipping listings fetch")
        return {}

    from concurrent.futures import ThreadPoolExecutor, as_completed

    out = {}
    max_workers = max(1, min(int(MAX_WORKERS or 4), 10))
    # batch size: small multiple of workers to reduce bursts
    batch_size = max(10, max_workers * 5)
    backoff_seconds = 0

    for batch_idx, batch in enumerate(chunk(skus, batch_size)):
        # apply backoff before starting the batch if needed
        if backoff_seconds:
            sleep_for = min(backoff_seconds, 60)
            print(f"[listings] Backing off for {sleep_for}s before batch {batch_idx+1}")
            time.sleep(sleep_for)

        quota_count = 0
        with ThreadPoolExecutor(max_workers=max_workers) as ex:
            futures = {ex.submit(fetch_listing_title, sku): sku for sku in batch}
            for fut in as_completed(futures):
                sku = futures[fut]
                try:
                    res = fut.result()
                except Exception as e:
                    print(f"[listings] Error fetching title for {sku}: {e}")
                    res = None

                # handle quota sentinel
                if isinstance(res, dict) and res.get("_quota"):
                    quota_count += 1
                    out[sku] = None
                else:
                    out[sku] = res

        # If we saw quota errors, increase backoff (exponential-ish)
        if quota_count:
            # base backoff 2s per quota hit, but cap and scale down by batch size
            backoff_seconds = max(2, min(30, 2 * (quota_count // max(1, max_workers))))
            # small additional sleep to let tokens refill
            time.sleep(0.5)
        else:
            # no quota errors => decay any previous backoff
            backoff_seconds = max(0, backoff_seconds // 2)
            # gentle pause between batches to avoid spikes
            time.sleep(0.1)

    # Ensure every requested SKU has an entry
    for sku in skus:
        if sku not in out:
            out[sku] = None

    return out
