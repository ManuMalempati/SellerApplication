"""
FBA Data Update Script

This script fetches FBA inventory data and updates the ProductMapping table.
Designed to be run periodically (e.g., every 1 hour) via task scheduler.
"""

import asyncio
import sys
import os
from datetime import datetime

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from . import config
config.load_env()

from app.fba.main import fba_report


async def run_fba_update():
    """
    Run the FBA data update process.
    Fetches all FBA data and saves it to the ProductMapping table.
    """
    print("=" * 60)
    print(f"FBA DATA UPDATE - Started at {datetime.now().isoformat()}")
    print("=" * 60)
    
    try:
        rows = await fba_report(save_to_db=True)
        
        print("=" * 60)
        print(f"FBA DATA UPDATE - Completed at {datetime.now().isoformat()}")
        print(f"Total rows processed: {len(rows)}")
        print("=" * 60)
        
        return True
        
    except Exception as e:
        print("=" * 60)
        print(f"FBA DATA UPDATE - FAILED at {datetime.now().isoformat()}")
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
