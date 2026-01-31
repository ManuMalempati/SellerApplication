#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone

from app.orders import get_orders
from app.database import connect_database
from app.database import robust_upsert_order_items  # your MERGE-based UPSERT

# -------------------------------------------------------------------
# Environment
# -------------------------------------------------------------------

BACKFILL_CHUNK_DAYS = int(os.getenv("BACKFILL_CHUNK_DAYS", "1"))
SYNC_OVERLAP_HOURS = int(os.getenv("SYNC_OVERLAP_HOURS", "2"))

ORDERS_RETRIES = int(os.getenv("ORDERS_RETRIES", "8"))
ORDERS_BACKOFF_SECONDS = float(os.getenv("ORDERS_BACKOFF_SECONDS", "4"))
ORDERS_BACKOFF_MULTIPLIER = float(os.getenv("ORDERS_BACKOFF_MULTIPLIER", "2.5"))
ORDERS_BACKOFF_JITTER = float(os.getenv("ORDERS_BACKOFF_JITTER", "1.0"))


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def fmt(dt: datetime) -> str:
    """Format datetime as ISO8601 Zulu."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def fetch_with_retries(params):
    """Retry wrapper for get_orders(params)."""
    delay = ORDERS_BACKOFF_SECONDS

    for attempt in range(1, ORDERS_RETRIES + 1):
        try:
            return await get_orders(params)
        except Exception as exc:
            print(f"Fetch error on attempt {attempt}/{ORDERS_RETRIES}: {exc}")

            if attempt == ORDERS_RETRIES:
                print("Max retries reached. Returning empty list.")
                return []

            time.sleep(delay)
            delay = delay * ORDERS_BACKOFF_MULTIPLIER + ORDERS_BACKOFF_JITTER

# -------------------------------------------------------------------
# Backfill Logic
# -------------------------------------------------------------------

async def run_backfill(start_date: datetime, end_date: datetime):
    """
    Backfills orders from start_date to end_date in windows of BACKFILL_CHUNK_DAYS.
    """

    print("Backfill starting")
    print(f"Start: {fmt(start_date)}")
    print(f"End:   {fmt(end_date)}")
    print(f"Window size: {BACKFILL_CHUNK_DAYS} days")

    window_start = start_date
    total_upserted = 0
    window_index = 0

    while window_start < end_date:
        window_end = min(window_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        window_index += 1

        params = {
            "CreatedAfter": fmt(window_start),
            "CreatedBefore": fmt(window_end),
            "MaxResultsPerPage": 100,
        }

        print("\n------------------------------------------------------------")
        print(f"Window {window_index}: {params['CreatedAfter']} -> {params['CreatedBefore']}")
        print("Fetching...")

        rows = await fetch_with_retries(params)
        print(f"Fetched {len(rows)} rows")

        # ---------------- DB UPSERT BLOCK ----------------
        try:
            conn = connect_database()
            cursor = conn.cursor()

            upserted = 0
            for row in rows:
                try:
                    robust_upsert_order_items(cursor, row)
                    upserted += 1
                except Exception as exc:
                    print(f"Upsert error: {exc}")

            conn.commit()
            cursor.close()
            conn.close()

            print(f"Upserted {upserted} rows")
            total_upserted += upserted

        except Exception as exc:
            print(f"Database error: {exc}")

        window_start = window_end

    print("\n============================================================")
    print(f"Backfill complete. Total rows upserted: {total_upserted}")
    print("============================================================")


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

def main():
    """
    Backfill from N days ago until now, with overlap.
    """

    now = datetime.now(timezone.utc)
    end_date = now - timedelta(hours=SYNC_OVERLAP_HOURS)

    # Default: backfill 365 days unless user overrides
    days_back = int(os.getenv("BACKFILL_DAYS", "31"))
    start_date = end_date - timedelta(days=days_back)

    start = time.time()
    asyncio.run(run_backfill(start_date, end_date))
    elapsed = time.time() - start

    print(f"\nFinished in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()