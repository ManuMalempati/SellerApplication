import time
import asyncio
from concurrent.futures import ThreadPoolExecutor

from ..auth import spapi_request
from ..fba.config import MARKETPLACE_ID
from ..fba.rate_limiter import pricing_limiter
from ..fba.helpers import retry_api_call, chunk, progress_lock, pricing_progress


def _pricing_call_batch(sku_list):
    pricing_limiter.acquire()

    params = {
        "MarketplaceId": MARKETPLACE_ID,
        "ItemType": "Sku",
        "Skus": ",".join(sku_list),
        "OfferType": "B2C",
        "ItemCondition": "New",
    }

    return spapi_request("GET", "/products/pricing/v0/price", params=params)


def fetch_pricing_batch(sku_list):
    resp = retry_api_call(_pricing_call_batch, sku_list)

    if isinstance(resp, dict) and "errors" in resp:
        print(f"[pricing] ERROR for batch {sku_list}: {resp['errors']}")
        return {sku: None for sku in sku_list}

    if not isinstance(resp, dict):
        print(f"[pricing] Non-dict response for batch {sku_list}: {resp}")
        return {sku: None for sku in sku_list}

    out = {}
    payload = resp.get("payload") or []

    for entry in payload:
        sku = entry.get("SellerSKU")
        offers = entry.get("Product", {}).get("Offers") or []

        if not sku:
            continue

        if not offers:
            out[sku] = None
            continue

        amount = (
            offers[0]
            .get("BuyingPrice", {})
            .get("ListingPrice", {})
            .get("Amount")
        )

        out[sku] = float(amount) if amount is not None else None

    for sku in sku_list:
        if sku not in out:
            print(f"[pricing] Missing SKU in payload: {sku}")
            out[sku] = None

    return out


async def run_pricing_batch(skus):
    pricing_progress["done"] = 0
    pricing_progress["total"] = len(skus)
    last_printed_pct = -1

    print(f"Fetching prices for {len(skus)} SKUs...")

    loop = asyncio.get_event_loop()
    results = {}

    with ThreadPoolExecutor(max_workers=1) as ex:
        for batch in chunk(skus, 20):
            batch_dict = await loop.run_in_executor(ex, fetch_pricing_batch, batch)
            results.update(batch_dict)

            with progress_lock:
                pricing_progress["done"] += len(batch)
                done = pricing_progress["done"]
                total = pricing_progress["total"]
                current_pct = 100 * done // total
                
                if current_pct // 25 > last_printed_pct // 25:
                    print(f"Pricing progress: {done}/{total} ({current_pct}%)")
                    last_printed_pct = current_pct

            time.sleep(2)

    print("Pricing batch complete.")
    return results
