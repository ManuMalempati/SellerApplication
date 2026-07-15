"""
FBA Data Update Script

This script fetches FBA inventory data and updates the FBAProductSummary table.
Designed to be run periodically (e.g., every 1 hour) via task scheduler.
"""

import asyncio
import sys
from app.utilities.utils import get_now_iso_string_with_custom_utc_offset
from app.fba.main import fba_report


async def run_fba_update():
    """
    Run the FBA data update process.
    Fetches all FBA data and saves it to the ProductMapping table.
    """
    print("=" * 60)
    print(f"FBA DATA UPDATE - Started at {get_now_iso_string_with_custom_utc_offset()}")
    print("=" * 60)
    
    try:
        rows = await fba_report(save_to_db=True)
        
        print("=" * 60)
        print(f"FBA DATA UPDATE - Completed at {get_now_iso_string_with_custom_utc_offset()}")
        print(f"Total rows processed: {len(rows)}")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print("=" * 60)
        print(f"FBA DATA UPDATE - FAILED at {get_now_iso_string_with_custom_utc_offset()}")
        print(f"Error: {e}")
        print("=" * 60)
        raise


def main():
    """Entry point for the FBA data update script."""
    try:
        asyncio.run(run_fba_update())
        sys.exit(0)
    except Exception as e:
        print(f"Fatal error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
