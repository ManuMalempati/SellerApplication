# RESPONSIBLE FOR FBAReimbursements Table

from app.database import connect_database
from app.utilities.utils import clean_str, safe_int, safe_float, safe_dt, now_utc_plus_offset_naive
from app.database import retry_deadlock
from app.utilities.fetch_report import fetch_spapi_report   # <-- unified fetcher

# ---------------------------------------------------------
# Fetch FBA Reimbursements Report (using unified fetcher)
# ---------------------------------------------------------

def fetch_fba_reimbursements(days=365):
    print(f"[FBA-REIMB] Fetching reimbursements for last {days} days...")

    # Use unified fetcher → returns parsed TSV rows
    rows = fetch_spapi_report(
        report_type="GET_FBA_REIMBURSEMENTS_DATA",
        days=days,
        output_type="tsv"
    )

    print(f"[FBA-REIMB] Parsed {len(rows)} rows")
    return rows


# ---------------------------------------------------------
# DELETE-AND-REPLACE UPSERT + AGG → ORDERITEMS
# ---------------------------------------------------------

def upsert_fba_reimbursements(rows):

    if not rows:
        print("[FBA-REIMB] No rows to upsert.")
        return 0

    conn = connect_database()
    cursor = conn.cursor()

    reimb_ids = sorted({
        clean_str(r.get("reimbursement-id"))
        for r in rows
        if clean_str(r.get("reimbursement-id"))
    })

    print(f"[FBA-REIMB] Deleting existing rows for {len(reimb_ids)} reimbursement-ids")

    if reimb_ids:
        retry_deadlock(
            lambda: cursor.execute(
                "DELETE FROM spapi_app_user.FBAReimbursements WHERE reimbursement_id IN (%s)" %
                ",".join("?" for _ in reimb_ids),
                reimb_ids
            ),
            label="DELETE FBAReimbursements"
        )
        conn.commit()

    print("[FBA-REIMB] Inserting fresh rows...")

    staging = []
    for r in rows:
        staging.append((
            clean_str(r.get("reimbursement-id")),
            safe_dt(r.get("approval-date")),
            clean_str(r.get("case-id")),
            clean_str(r.get("amazon-order-id")),
            clean_str(r.get("reason")),
            clean_str(r.get("sku")),
            clean_str(r.get("fnsku")),
            clean_str(r.get("asin")),
            clean_str(r.get("product-name")),
            clean_str(r.get("condition")),
            clean_str(r.get("currency-unit")),
            safe_float(r.get("amount-per-unit")),
            safe_float(r.get("amount-total")),
            safe_int(r.get("quantity-reimbursed-cash")),
            safe_int(r.get("quantity-reimbursed-inventory")),
            safe_int(r.get("quantity-reimbursed-total")),
            clean_str(r.get("original-reimbursement-id")),
            clean_str(r.get("original-reimbursement-type")),
            now_utc_plus_offset_naive(),
            now_utc_plus_offset_naive(),
        ))

    cursor.fast_executemany = True

    retry_deadlock(
        lambda: cursor.executemany("""
            INSERT INTO spapi_app_user.FBAReimbursements (
                reimbursement_id,
                approval_date,
                case_id,
                amazon_order_id,
                reason,
                sku,
                fnsku,
                asin,
                product_name,
                condition,
                currency_unit,
                amount_per_unit,
                amount_total,
                quantity_reimbursed_cash,
                quantity_reimbursed_inventory,
                quantity_reimbursed_total,
                original_reimbursement_id,
                original_reimbursement_type,
                created_at,
                updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, staging),
        label="INSERT FBAReimbursements"
    )

    # Aggregate reimbursements
    retry_deadlock(
        lambda: cursor.execute("""
            IF OBJECT_ID('tempdb..#AggReimb') IS NOT NULL DROP TABLE #AggReimb;

            SELECT
                amazon_order_id,
                sku,
                SUM(amount_total) AS total_amount,
                SUM(quantity_reimbursed_cash) AS qty_cash,
                SUM(quantity_reimbursed_inventory) AS qty_inv,
                SUM(quantity_reimbursed_total) AS qty_total,
                MAX(approval_date) AS approval_date
            INTO #AggReimb
            FROM spapi_app_user.FBAReimbursements
            GROUP BY amazon_order_id, sku;
        """),
        label="AGG FBAReimbursements"
    )

    # Update OrderItems
    retry_deadlock(
        lambda: cursor.execute("""
            UPDATE O
            SET 
                O.Reimbursed = A.total_amount,
                O.ReimbDate  = A.approval_date
            FROM OrderItems O
            JOIN #AggReimb A
              ON O.AmazonOrderId = A.amazon_order_id
             AND O.SKU           = A.sku;
        """),
        label="UPDATE OrderItems reimbursements"
    )

    conn.commit()
    cursor.close()
    conn.close()

    print(f"[FBA-REIMB] Inserted {len(staging)} rows (raw) and updated OrderItems with aggregated reimbursements")
    return len(staging)


# ---------------------------------------------------------
# Main
# ---------------------------------------------------------

def run_reimbursements_import(days=365):
    print("==============================================")
    print("FBA REIMBURSEMENTS IMPORT - START")
    print("==============================================")

    rows = fetch_fba_reimbursements(days)
    upsert_fba_reimbursements(rows)

    print("==============================================")
    print("FBA REIMBURSEMENTS IMPORT - COMPLETE")
    print("==============================================")

if __name__ == "__main__":
    run_reimbursements_import(days=365)