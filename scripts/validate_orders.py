import argparse
import time
from datetime import datetime, timedelta
from typing import Optional
import os

# Import from repo root (run from project root)
from app.auth import spapi_request  # if you prefer package style, set PYTHONPATH=. and use SellerAPIApplication.auth

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
MAX_RESULTS_PER_PAGE = 100
RETRIES = 3
BACKOFF_SECONDS = 2
# Safety offset so LastUpdatedBefore is not “too fresh”
DEFAULT_BEFORE_OFFSET_MINUTES = 5


def iso_utc_now():
    return datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


def iso_utc_minus(delta: timedelta):
    return (datetime.utcnow().replace(microsecond=0) - delta).isoformat() + "Z"


def fetch_orders_window(last_updated_after: str, last_updated_before: Optional[str]):
    orders = []
    next_token = None
    page = 0

    while True:
        params = {"MaxResultsPerPage": MAX_RESULTS_PER_PAGE}
        if next_token:
            params["NextToken"] = next_token
        else:
            params["LastUpdatedAfter"] = last_updated_after
            if last_updated_before:
                params["LastUpdatedBefore"] = last_updated_before
            if MARKETPLACE_ID:
                params["MarketplaceIds"] = [MARKETPLACE_ID]

        attempt = 0
        while True:
            attempt += 1
            resp = spapi_request("GET", "/orders/v0/orders", params=params)
            if "errors" not in resp:
                break
            if attempt >= RETRIES:
                print(f"[page {page}] ERROR after {attempt} attempts: {resp.get('errors')}")
                return orders
            print(f"[page {page}] transient error, retrying in {BACKOFF_SECONDS}s: {resp.get('errors')}")
            time.sleep(BACKOFF_SECONDS)

        payload = resp.get("payload") or {}
        page_orders = payload.get("Orders", [])
        orders.extend(page_orders)
        page += 1

        print(f"Fetched page {page}, got {len(page_orders)} orders, total so far {len(orders)}")

        next_token = payload.get("NextToken")
        if not next_token:
            break

    print(f"Done. Pages: {page}, Total orders: {len(orders)}")
    return orders


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=5, help="Lookback window in days for LastUpdatedAfter")
    parser.add_argument("--hours", type=int, default=0)
    parser.add_argument("--minutes", type=int, default=0)
    parser.add_argument("--before-now", action="store_true", help="Set LastUpdatedBefore to now minus offset (UTC)")
    parser.add_argument("--before-offset-minutes", type=int, default=DEFAULT_BEFORE_OFFSET_MINUTES,
                        help="Minutes to subtract from now for LastUpdatedBefore")
    args = parser.parse_args()

    delta = timedelta(days=args.days, hours=args.hours, minutes=args.minutes)
    last_updated_after = iso_utc_minus(delta)
    last_updated_before = None
    if args.before_now:
        last_updated_before = iso_utc_minus(timedelta(minutes=args.before_offset_minutes))

    print(f"MarketplaceIds: {MARKETPLACE_ID}")
    print(f"LastUpdatedAfter: {last_updated_after}")
    if last_updated_before:
        print(f"LastUpdatedBefore: {last_updated_before}")
    print(f"MaxResultsPerPage: {MAX_RESULTS_PER_PAGE}")

    fetch_orders_window(last_updated_after, last_updated_before)


if __name__ == "__main__":
    main()