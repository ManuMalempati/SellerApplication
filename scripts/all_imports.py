#!/usr/bin/env python3
import sys
from datetime import datetime, timezone, timedelta
import os

# Import your individual pipelines
from app.returns.returns import run_returns_import
from app.returns.reimbursements import run_reimbursements_import
from app.returns.removal import run_removal_orders_import
from app.returns.removalshipments import run_removal_shipments_import

# Default days (environment override supported)
days = int(os.getenv("RETURNS_DATA_DAYS", 35))

def now_utc_plus_4():
    """Return timezone-aware datetime in UTC+4."""
    return (datetime.now(timezone.utc) + timedelta(hours=4)).isoformat()


def run_all_imports(days=365):
    print("==============================================")
    print(f"RUNNING ALL FBA IMPORTS FOR LAST {days} DAYS")
    print(f"START TIME: {now_utc_plus_4()}")
    print("==============================================\n")

    try:
        print(">>> Running RETURNS import...")
        run_returns_import(days=days)
        print(">>> RETURNS import complete.\n")

        print(">>> Running REIMBURSEMENTS import...")
        run_reimbursements_import(days=days)
        print(">>> REIMBURSEMENTS import complete.\n")

        print(">>> Running REMOVAL ORDERS import...")
        run_removal_orders_import(days=days)
        print(">>> REMOVAL ORDERS import complete.\n")

        print(">>> Running REMOVAL SHIPMENTS import...")
        run_removal_shipments_import(days=days)
        print(">>> REMOVAL SHIPMENTS import complete.\n")

    except Exception as e:
        print("==============================================")
        print("FATAL ERROR IN RUN_ALL_IMPORTS")
        print("==============================================")
        print(e)
        sys.exit(1)

    print("==============================================")
    print("ALL IMPORTS COMPLETED SUCCESSFULLY")
    print(f"END TIME: {now_utc_plus_4()}")
    print("==============================================")
    sys.exit(0)


if __name__ == "__main__":
    run_all_imports(days=days)
