#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone
import config
from app.orders.orders import get_orders
from app.database import connect_database
from app.orders.database_orders import replace_order_items_for_order
from app.utils import convert_utc_to_utcz_string

# -------------------------------------------------------------------
# Environment (from config)
# -------------------------------------------------------------------
BACKFILL_CHUNK_DAYS = config.BACKFILL_CHUNK_DAYS
SYNC_OVERLAP_HOURS = config.SYNC_OVERLAP_HOURS

# -------------------------------------------------------------------
# Backfill Logic
# -------------------------------------------------------------------
async def run_backfill(start_date: datetime, end_date: datetime):
    print("Backfill starting")
    print(f"Range: {convert_utc_to_utcz_string(start_date)} to {convert_utc_to_utcz_string(end_date)}")
    print(f"Chunk Size: {BACKFILL_CHUNK_DAYS} days")

    window_start = start_date
    total_upserted = 0
    window_index = 0

    while window_start < end_date:
        window_end = min(window_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        window_index += 1

        params = {
            "CreatedAfter": convert_utc_to_utcz_string(window_start),
            "CreatedBefore": convert_utc_to_utcz_string(window_end),
            "MaxResultsPerPage": 100,
        }

        print(f"\n[{window_index}] Window: {params['CreatedAfter']} -> {params['CreatedBefore']}")
        
        try:
            # get_orders handles its own retries internally
            rows = await get_orders(params)
            print(f"Fetched {len(rows)} rows")

            if not rows:
                window_start = window_end
                continue

            # Group rows by AmazonOrderId for the delete-and-replace logic
            grouped = {}
            for row in rows:
                oid = row.get("AmazonOrderId")
                if oid:
                    grouped.setdefault(oid, []).append(row)

            conn = connect_database()
            cursor = conn.cursor()

            upserted = 0
            for oid, group in grouped.items():
                replace_order_items_for_order(cursor, oid, group)
                upserted += len(group)

            conn.commit()
            cursor.close()
            conn.close()

            print(f"Successfully upserted {upserted} rows")
            total_upserted += upserted

        except Exception as exc:
            print(f"Error processing window {window_index}: {exc}")
            # We continue to the next window so one failure doesn't kill the whole backfill
            pass

        window_start = window_end

    print("\n" + "="*40)
    print(f"Backfill complete. Total: {total_upserted}")
    print("="*40)

# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------
def main():
    now = datetime.now(timezone.utc)
    end_date = now - timedelta(hours=SYNC_OVERLAP_HOURS)

    days_back = int(os.getenv("BACKFILL_DAYS", "37"))
    start_date = end_date - timedelta(days=days_back)

    start_timer = time.time()
    asyncio.run(run_backfill(start_date, end_date))
    
    elapsed = time.time() - start_timer
    print(f"\nFinished in {elapsed:.1f} seconds")

if __name__ == "__main__":
    main()