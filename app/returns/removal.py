"""
FBARemovalOrders Ingestion Pipeline
===================================

This module imports the `GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA`
SP‑API report and populates the `spapi_app_user.FBARemovalOrders` table with
a clean, normalized snapshot of all FBA removal order activity.

Pipeline Responsibilities
-------------------------

1. Fetch Removal Order Detail Report
   - Downloads the FBA removal order detail report for the last N days.
   - Parses TSV rows into structured dictionaries.

2. Delete‑and‑Replace Upsert
   - Extracts all unique `order-id` values from the incoming dataset.
   - Deletes existing rows for those order IDs (deadlock‑safe).
   - Inserts fresh rows with:
        • order_id, sku, fnsku  
        • disposition, order_type, service_speed  
        • request_date, last_updated_date  
        • quantities (requested, cancelled, disposed, shipped, in‑process)
        • removal_fee and currency  
        • created_at / updated_at timestamps  

3. Output
   - Returns the number of rows inserted.
   - Ensures the `FBARemovalOrders` table always reflects the latest removal
     order data with no duplicates or stale entries.

Removals are the requests sent by the business to send the stock from the fulfillment centre to
the business due to various reasons such as the item not being very profitable and has high storage fees
at fulfillment centre.
"""

from app.database import connect_database, retry_deadlock
from app.utilities.utils import clean_str, safe_int, safe_float, safe_dt, now_utc_plus_offset_naive
from app.utilities.fetch_report import fetch_spapi_report   # unified fetcher


# ---------------------------------------------------------
# Fetch FBA Removal Order Detail Report (Unified Fetcher)
# ---------------------------------------------------------

def fetch_fba_removal_orders(days=365):
    print(f"[FBA-REMOVAL] Fetching removal orders for last {days} days...")

    rows = fetch_spapi_report(
        report_type="GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA",
        days=days,
        output_type="tsv"
    )

    print(f"[FBA-REMOVAL] Parsed {len(rows)} rows")
    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT (DEADLOCK SAFE)
# ---------------------------------------------------------

def upsert_fba_removal_orders(rows):

    if not rows:
        print("[FBA-REMOVAL] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    # ---------------------------------------------------------
    # Delete existing rows for these order-ids
    # ---------------------------------------------------------
    order_ids = sorted({
        clean_str(r.get("order-id"))
        for r in rows
        if clean_str(r.get("order-id"))
    })

    print(f"[FBA-REMOVAL] Deleting existing rows for {len(order_ids)} order-ids")

    if order_ids:
        retry_deadlock(
            lambda: cursor.execute(
                "DELETE FROM spapi_app_user.FBARemovalOrders WHERE order_id IN (%s)" %
                ",".join("?" for _ in order_ids),
                order_ids
            ),
            label="DELETE FBARemovalOrders"
        )
        conn.commit()

    # ---------------------------------------------------------
    # Insert fresh rows
    # ---------------------------------------------------------
    print("[FBA-REMOVAL] Inserting fresh rows...")

    staging = []
    for r in rows:
        staging.append((
            clean_str(r.get("order-id")),
            clean_str(r.get("sku")),
            clean_str(r.get("disposition")),
            safe_dt(r.get("request-date")),
            clean_str(r.get("order-type")),
            clean_str(r.get("service-speed")),
            clean_str(r.get("order-status")),
            safe_dt(r.get("last-updated-date")),
            clean_str(r.get("fnsku")),
            safe_int(r.get("requested-quantity")),
            safe_int(r.get("cancelled-quantity")),
            safe_int(r.get("disposed-quantity")),
            safe_int(r.get("shipped-quantity")),
            safe_int(r.get("in-process-quantity")),
            safe_float(r.get("removal-fee")),
            clean_str(r.get("currency")),
            now_utc_plus_offset_naive(),   # created_at
            now_utc_plus_offset_naive(),   # updated_at
        ))

    cursor.fast_executemany = True

    retry_deadlock(
        lambda: cursor.executemany("""
            INSERT INTO spapi_app_user.FBARemovalOrders (
                order_id,
                sku,
                disposition,
                request_date,
                order_type,
                service_speed,
                order_status,
                last_updated_date,
                fnsku,
                requested_quantity,
                cancelled_quantity,
                disposed_quantity,
                shipped_quantity,
                in_process_quantity,
                removal_fee,
                currency,
                created_at,
                updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, staging),
        label="INSERT FBARemovalOrders"
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FBA-REMOVAL] Inserted {len(staging)} rows")
    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_removal_orders_import(days=365):
    print("==============================================")
    print("FBA REMOVAL ORDERS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_removal_orders(days)
    upsert_fba_removal_orders(rows)

    print("==============================================")
    print("FBA REMOVAL ORDERS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":
    run_removal_orders_import(days=365)