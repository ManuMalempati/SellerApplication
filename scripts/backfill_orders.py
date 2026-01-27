#!/usr/bin/env python3
"""
backfill_orders.py

Backfill orders for a past period (default: 1 year) using the Orders API (no reports).
This script calls the existing async `get_orders` routine for contiguous time windows
and saves each window's output as a JSON file under ./backfill_outputs/.

Usage:
    python -m app.backfill_orders
    (or) python app/backfill_orders.py

Config (via environment or modify constants below):
- BACKFILL_DAYS: total days to backfill (default 365)
- WINDOW_DAYS: window size in days per API call (default 7)
- SLEEP_BETWEEN_WINDOWS: seconds to sleep between windows (default 2)
- OUTPUT_DIR: directory to write JSON files (default ./backfill_outputs)

Notes:
- This script does NOT attempt any DB upsert. It saves the rows returned from get_orders
  to JSON files for manual review/loading. If you want DB upsert behavior, add it where
  indicated.
- Run this on a machine with your environment (LWA creds, SPAPI env vars) configured.
"""
from __future__ import annotations
import os
import json
import time
import asyncio
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional

# Import the existing get_orders implementation (async)
from .orders import get_orders

# Configuration via environment with sensible defaults
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "365"))
WINDOW_DAYS = int(os.getenv("WINDOW_DAYS", "7"))
SLEEP_BETWEEN_WINDOWS = float(os.getenv("SLEEP_BETWEEN_WINDOWS", "2.0"))
OUTPUT_DIRECTORY = os.getenv("BACKFILL_OUTPUT_DIR", "backfill_outputs")


def ensure_output_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)


def format_dt_z(dt: Optional[datetime]) -> Optional[str]:
    """Return canonical UTC Z timestamp like 2026-01-26T05:48:16Z."""
    if dt is None:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


async def run_backfill(
    days_back: int = BACKFILL_DAYS,
    window_days: int = WINDOW_DAYS,
    sleep_between_windows: float = SLEEP_BETWEEN_WINDOWS,
    out_dir: str = OUTPUT_DIRECTORY,
) -> List[str]:
    """
    Run the backfill in contiguous windows from (now - days_back) up to now.

    Returns list of filepaths written.
    """
    ensure_output_dir(out_dir)

    end_time_utc = datetime.now(timezone.utc)
    start_time_utc = end_time_utc - timedelta(days=days_back)

    window_start = start_time_utc
    written_files: List[str] = []

    # Estimate number of windows and approximate time
    total_windows = max(1, int((days_back + window_days - 1) // window_days))
    approx_seconds_per_window = max(5, WINDOW_DAYS * 2)
    approx_total_seconds = total_windows * approx_seconds_per_window
    print(f"🔄 Backfill from {format_dt_z(start_time_utc)} to {format_dt_z(end_time_utc)}")
    print(f"🔢 Window size: {window_days} days, windows: {total_windows}")
    print(f"⏱ Approximate total time: {approx_total_seconds:.0f}s (~{approx_total_seconds/60:.1f}m)")

    window_index = 0
    while window_start < end_time_utc:
        window_end = min(window_start + timedelta(days=window_days), end_time_utc)

        # Build params for Orders API call. Prefer CreatedAfter/CreatedBefore to fetch orders by creation date.
        params = {
            "CreatedAfter": format_dt_z(window_start),
            "CreatedBefore": format_dt_z(window_end),
            "MaxResultsPerPage": 100,
        }

        window_index += 1
        print(f"\n[{window_index}/{total_windows}] Fetching orders for {params['CreatedAfter']} → {params['CreatedBefore']}")

        try:
            # Start timing BEFORE calling get_orders
            window_start_time = time.time()

            rows = await get_orders(params=params, db_cursor=None)

            window_elapsed = time.time() - window_start_time
            print(f"⏱ Window completed in {window_elapsed:.1f}s")

        except Exception as exc:
            print(f"❌ Exception while fetching window {params['CreatedAfter']} → {params['CreatedBefore']}: {exc}")
            rows = []


        # Save results to JSON file
        safe_start = params["CreatedAfter"].replace(":", "").replace("-", "")
        safe_end = params["CreatedBefore"].replace(":", "").replace("-", "")
        filename = f"backfill_{safe_start}_to_{safe_end}.json"
        filepath = os.path.join(out_dir, filename)

        try:
            with open(filepath, "w", encoding="utf-8") as fh:
                json.dump(
                    {
                        "window_start": params["CreatedAfter"],
                        "window_end": params["CreatedBefore"],
                        "rows_count": len(rows),
                        "rows": rows,
                    },
                    fh,
                    ensure_ascii=False,
                    indent=2,
                )
            written_files.append(filepath)
            print(f"✅ Wrote {len(rows)} rows to {filepath}")
        except Exception as exc:
            print(f"❌ Failed to write {filepath}: {exc}")

        # polite sleep between windows
        time.sleep(sleep_between_windows)
        window_start = window_end

    print("\n🔚 Backfill complete.")
    return written_files


def main():
    # Allow overriding via env vars for a single-run
    days_back = int(os.getenv("BACKFILL_DAYS", BACKFILL_DAYS))
    window_days = int(os.getenv("WINDOW_DAYS", WINDOW_DAYS))
    sleep_seconds = float(os.getenv("SLEEP_BETWEEN_WINDOWS", SLEEP_BETWEEN_WINDOWS))
    out_dir = os.getenv("BACKFILL_OUTPUT_DIR", OUTPUT_DIRECTORY)

    start_run_time = time.time()
    written = asyncio.run(run_backfill(days_back, window_days, sleep_seconds, out_dir))
    elapsed = time.time() - start_run_time
    print(f"\nSummary: wrote {len(written)} files to {out_dir} in {elapsed:.1f}s")


if __name__ == "__main__":
    main()