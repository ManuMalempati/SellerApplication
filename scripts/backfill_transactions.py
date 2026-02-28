#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database, upsert_financial_transactions
from app.utils import convert_utc_to_utcz_string

# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------
BACKFILL_CHUNK_DAYS = config.BACKFILL_CHUNK_DAYS
SYNC_OVERLAP_HOURS = config.SYNC_OVERLAP_HOURS

# ---------------------------------------------------------
# Backfill Logic
# ---------------------------------------------------------

async def run_backfill(start_date: datetime, end_date: datetime):
    print("="*60)
    print(f"FINANCIAL BACKFILL STARTING: {convert_utc_to_utcz_string(start_date)} -> {convert_utc_to_utcz_string(end_date)}")
    print("="*60)

    window_start = start_date
    total_upserted = 0
    window_index = 0

    while window_start < end_date:
        window_end = min(window_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        window_index += 1

        params = {
            "postedAfter": convert_utc_to_utcz_string(window_start),
            "postedBefore": convert_utc_to_utcz_string(window_end),
        }

        print(f"\n[Window {window_index}] {params['postedAfter']} to {params['postedBefore']}")
        
        # 1. Fetch data
        # If get_transactions fails (API error, timeout, etc.), it will crash here.
        conn = connect_database()
        cursor = conn.cursor()
        
        print("Fetching from SP-API...")
        rows = get_transactions(params=params, db_cursor=cursor)
        
        cursor.close()
        conn.close()

        print(f"Fetched {len(rows)} rows.")

        if not rows:
            print("No transactions found in this window. Moving to next.")
            window_start = window_end
            continue

        # 2. Upsert data
        # If the database or the upsert logic fails, it will crash here.
        print(f"Upserting {len(rows)} rows...")
        upserted = upsert_financial_transactions(rows)
        
        print(f"Success. Upserted {upserted} rows.")
        total_upserted += upserted
        window_start = window_end

    print("\n" + "="*60)
    print(f"BACKFILL COMPLETE | Total Upserted: {total_upserted}")
    print("="*60)


def main():
    # Setup time range
    now = datetime.now(timezone.utc)
    end_date = now - timedelta(hours=SYNC_OVERLAP_HOURS)
    
    # Default to 56 days if not in ENV
    days_back = int(os.getenv("BACKFILL_DAYS", "56"))
    start_date = end_date - timedelta(days=days_back)

    start_timer = time.time()
    
    # No try/except here; if run_backfill raises an error, the script exits with a traceback.
    asyncio.run(run_backfill(start_date, end_date))
    
    elapsed = time.time() - start_timer
    print(f"\nExecution Time: {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()