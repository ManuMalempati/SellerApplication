#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database
from app.database import upsert_financial_transactions   # shared optimized upsert


BACKFILL_CHUNK_DAYS = config.BACKFILL_CHUNK_DAYS
SYNC_OVERLAP_HOURS = config.SYNC_OVERLAP_HOURS


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def fmt(dt: datetime) -> str:
    """
    Format datetime as ISO8601 Zulu for SP-API.
    Always output UTC Z timestamps.
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (
        dt.astimezone(timezone.utc)
          .replace(microsecond=0)
          .isoformat()
          .replace("+00:00", "Z")
    )


# ---------------------------------------------------------
# Backfill Logic
# ---------------------------------------------------------

async def run_backfill(start_date: datetime, end_date: datetime):
    print("Backfill starting")
    print(f"Start: {fmt(start_date)}")
    print(f"End:   {fmt(end_date)}")
    print(f"Window size: {BACKFILL_CHUNK_DAYS} days")

    window_start = start_date
    total_upserted = 0
    window_index = 0

    while window_start < end_date:

        window_end = min(
            window_start + timedelta(days=BACKFILL_CHUNK_DAYS),
            end_date
        )
        window_index += 1

        params = {
            "postedAfter": fmt(window_start),
            "postedBefore": fmt(window_end),
        }

        print("\n------------------------------------------------------------")
        print(f"Window {window_index}: {params['postedAfter']} -> {params['postedBefore']}")
        print("Fetching...")

        # Fetch transactions
        conn = connect_database()
        cur = conn.cursor()
        try:
            rows = get_transactions(params=params, db_cursor=cur)
        finally:
            cur.close()
            conn.close()

        print(f"Fetched {len(rows)} rows")

        if not rows:
            window_start = window_end
            continue

        # ⭐ Use shared optimized lifecycle-aware upsert
        print(f"Upserting window {window_index} via shared upsert...")
        upserted = upsert_financial_transactions(rows)
        print(f"Upserted {upserted} rows in window {window_index}")

        total_upserted += upserted
        window_start = window_end

    print("\n============================================================")
    print(f"Backfill complete. Total rows upserted: {total_upserted}")
    print("============================================================")


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------

def main():
    now = datetime.now(timezone.utc)
    end_date = now - timedelta(hours=SYNC_OVERLAP_HOURS)

    days_back = int(os.getenv("BACKFILL_DAYS", "56"))
    start_date = end_date - timedelta(days=days_back)

    start = time.time()
    asyncio.run(run_backfill(start_date, end_date))
    elapsed = time.time() - start

    print(f"\nFinished in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()