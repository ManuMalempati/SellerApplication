# RESPONSIBLE FOR FBARemovalShipments Table

import time
import csv
from io import StringIO
from datetime import datetime, timedelta, timezone

from app.database import connect_database, retry_deadlock
from app.utilities.utils import clean_str, safe_int, safe_dt, now_utc_plus_offset_naive
from app.utilities.fetch_report import fetch_spapi_report   # <-- unified fetcher


# ---------------------------------------------------------
# Fetch FBA Removal Shipment Detail Report (Unified Fetcher)
# ---------------------------------------------------------

def fetch_fba_removal_shipments(days=365):
    print(f"[FBA-REM-SHIP] Fetching removal shipments for last {days} days...")

    rows = fetch_spapi_report(
        report_type="GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA",
        days=days,
        output_type="tsv"
    )

    print(f"[FBA-REM-SHIP] Parsed {len(rows)} rows")
    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT (DEADLOCK SAFE)
# ---------------------------------------------------------

def upsert_fba_removal_shipments(rows):

    if not rows:
        print("[FBA-REM-SHIP] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    order_ids = sorted({
        clean_str(r.get("order-id"))
        for r in rows
        if clean_str(r.get("order-id"))
    })

    print(f"[FBA-REM-SHIP] Deleting existing rows for {len(order_ids)} order-ids")

    if order_ids:
        retry_deadlock(
            lambda: cursor.execute(
                "DELETE FROM spapi_app_user.FBARemovalShipments WHERE order_id IN (%s)" %
                ",".join("?" for _ in order_ids),
                order_ids
            ),
            label="DELETE FBARemovalShipments"
        )
        conn.commit()

    print("[FBA-REM-SHIP] Inserting fresh rows...")

    staging = []
    for r in rows:
        staging.append((
            clean_str(r.get("order-id")),
            clean_str(r.get("sku")),
            clean_str(r.get("disposition")),
            clean_str(r.get("tracking-number")),
            safe_dt(r.get("request-date")),
            safe_dt(r.get("shipment-date")),
            clean_str(r.get("fnsku")),
            safe_int(r.get("shipped-quantity")),
            clean_str(r.get("carrier")),
            clean_str(r.get("removal-order-type")),
            now_utc_plus_offset_naive(),   # created_at
            now_utc_plus_offset_naive(),   # updated_at
        ))

    cursor.fast_executemany = True

    retry_deadlock(
        lambda: cursor.executemany("""
            INSERT INTO spapi_app_user.FBARemovalShipments (
                order_id,
                sku,
                disposition,
                tracking_number,
                request_date,
                shipment_date,
                fnsku,
                shipped_quantity,
                carrier,
                removal_order_type,
                created_at,
                updated_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, staging),
        label="INSERT FBARemovalShipments"
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FBA-REM-SHIP] Inserted {len(staging)} rows")
    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_removal_shipments_import(days=365):
    print("==============================================")
    print("FBA REMOVAL SHIPMENTS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_removal_shipments(days)
    upsert_fba_removal_shipments(rows)

    print("==============================================")
    print("FBA REMOVAL SHIPMENTS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":
    run_removal_shipments_import(days=365)