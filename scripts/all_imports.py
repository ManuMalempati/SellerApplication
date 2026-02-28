#!/usr/bin/env python3
import sys
import os

# Import your individual pipelines
from app.returns.returns import run_returns_import
from app.returns.reimbursements import run_reimbursements_import
from app.returns.removal import run_removal_orders_import
from app.returns.removalshipments import run_removal_shipments_import
from .config import get_now_iso_string_with_custom_utc_offset

# Default days (environment override supported)
days = int(os.getenv("RETURNS_DATA_DAYS", 35))

def run_all_imports(days=365):
    print("==============================================")
    print(f"RUNNING ALL FBA IMPORTS FOR LAST {days} DAYS")
    print(f"START TIME: {get_now_iso_string_with_custom_utc_offset()}")
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
    print(f"END TIME: {get_now_iso_string_with_custom_utc_offset()}")
    print("==============================================")
    sys.exit(0)


if __name__ == "__main__":
    run_all_imports(days=days)
