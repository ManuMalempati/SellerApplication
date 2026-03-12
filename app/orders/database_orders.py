"""
Replace OrderItems for a Single Amazon Order (COG‑Preserving)
=============================================================

This function deletes and reinserts all `OrderItems` rows for a given
AmazonOrderId in an atomic, deadlock‑safe operation. It is used whenever an
order is updated or re‑fetched from SP‑API, ensuring the database always
reflects the latest state of the order while preserving accounting‑critical
fields.

Key Behaviors
-------------

1. Full Row Replacement (Per Order)
   - All existing rows for the given AmazonOrderId are deleted.
   - New rows are inserted exactly as produced by the ingestion pipeline.
   - This avoids partial updates, merge conflicts, and stale data.

2. COG Immutability Rule (IMPORTANT)
   - Cost of Goods (COG) must never change after an order item is first seen.
   - Before deleting old rows, the function loads existing COG values:
         SELECT SKU, COG FROM OrderItems WHERE AmazonOrderId = ?
   - During reinsertion:
         • If an old COG exists → it is reused  
         • If no old COG exists → the new COG is inserted  
   - This guarantees historical accounting accuracy even when orders are updated
     (refunds, returns, reimbursements, status changes, etc.).

3. Deadlock‑Safe Execution
   - All operations run inside `retry_deadlock()`, ensuring safe execution under
     concurrent ingestion or reporting workloads.

4. No Other Fields Are Preserved
   - Only COG is immutable.
   - All other fields (fees, VAT, profit, timestamps, statuses, etc.) are
     overwritten with the latest computed values from the ingestion pipeline.

"""

from app.database import retry_deadlock

def replace_order_items_for_order(cursor, amazon_order_id, rows):

    # -----------------------------------------------------
    # STEP 1 — Load existing COG values BEFORE deleting
    # -----------------------------------------------------
    cursor.execute("""
        SELECT SKU, COG
        FROM OrderItems
        WHERE AmazonOrderId = ?
    """, (amazon_order_id,))

    existing_cog_map = {sku: cog for sku, cog in cursor.fetchall()}

    def _do():
        # Delete old rows
        cursor.execute("DELETE FROM OrderItems WHERE AmazonOrderId = ?", (amazon_order_id,))

        if not rows:
            return

        sql = """
            INSERT INTO OrderItems (
                AmazonOrderId, OrderDate, SKU, ASIN, SSKU,
                Brand, Category, Title, Qty, UnitPrice, Subtotal, Currency,
                OrderStatus, LastUpdateDate, FeeIncl, FeePct, FBAFeesIncl,
                TotalFee, RVAT, VAT, COG, Profit,
                Refund, RefundDate, ReturnDate, ReturnDisposition, ReturnReason,
                LicensePlateNumber, Reimbursed, ReimbDate, RemovalDate, RemovalId,
                RemovalTracking, RemovalDelivery, FirstSeenAt, LastSeenAt
            )
            VALUES (
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?, ?,
                ?, ?, ?, ?, ?,
                ?, ?
            )
        """

        params = []
        for row in rows:

            sku = row["SKU"]

            # -----------------------------------------------------
            # STEP 2 — Preserve old COG if it existed
            # -----------------------------------------------------
            if sku in existing_cog_map and existing_cog_map[sku] is not None:
                row_cog = existing_cog_map[sku]
            else:
                row_cog = row["COG"]  # first time seeing this order item

            params.append((
                row["AmazonOrderId"],
                row["OrderDate"],
                row["SKU"],
                row["ASIN"],
                row["SSKU"],
                row["Brand"],
                row["Category"],
                row["Title"],
                row["Qty"],
                row["UnitPrice"],
                row["Subtotal"],
                row["Currency"],
                row["OrderStatus"],
                row["LastUpdateDate"],
                row["FeeIncl"],
                row["FeePct"],
                row["FBAFeesIncl"],
                row["TotalFee"],
                row["RVAT"],
                row["VAT"],
                row_cog,                 # <-- IMMUTABLE COG PRESERVED HERE
                row["Profit"],
                row["Refund"],
                row["RefundDate"],
                row["ReturnDate"],
                row["ReturnDisposition"],
                row["ReturnReason"],
                row["LicensePlateNumber"],
                row["Reimbursed"],
                row["ReimbDate"],
                row["RemovalDate"],
                row["RemovalId"],
                row["RemovalTracking"],
                row["RemovalDelivery"],
                row["FirstSeenAt"],
                row["LastSeenAt"],
            ))

        cursor.executemany(sql, params)

    retry_deadlock(_do, label=f"OrderItems({amazon_order_id})")