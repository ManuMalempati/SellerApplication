import asyncio
from concurrent.futures import ThreadPoolExecutor

from ..estimates import get_fees_estimate
from .config import MAX_WORKERS
from .rate_limiter import fees_limiter
from .helpers import retry_api_call, progress_lock, fees_progress


def _fees_call(sku, asin, price):
    fees_limiter.acquire()
    return get_fees_estimate(sku, asin, price)


def estimate_fees(sku, asin, price):
    return retry_api_call(_fees_call, sku, asin, price)


def estimate_fees_worker(sku, asin, price):
    result = (sku, asin, price), estimate_fees(sku, asin, price)
    with progress_lock:
        fees_progress["done"] += 1
        done = fees_progress["done"]
        total = fees_progress["total"]
        if total and (done % 20 == 0 or done == total):
            print(f"Fees progress: {done}/{total} ({100 * done // total}%)")
    return result


async def run_fees_batch(items):
    fees_progress["done"] = 0
    fees_progress["total"] = len(items)

    print(f"Estimating fees for {len(items)} items...")

    if not items:
        print("No items to estimate fees for.")
        return {}

    loop = asyncio.get_event_loop()
    results = {}

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        tasks = [
            loop.run_in_executor(ex, estimate_fees_worker, sku, asin, price)
            for (sku, asin, price) in items
        ]
        batch_results = await asyncio.gather(*tasks)

    for d in batch_results:
        key, val = d
        results[key] = val

    print("Fee batch complete.")
    return results
