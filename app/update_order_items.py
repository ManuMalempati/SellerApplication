#!/usr/bin/env python3
"""
recalculate_items.py — Recalculate COG + Profit for specific SKUs
without touching existing fee fields.
"""

import os
import sys

# Add project root to path if running standalone
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dotenv import load_dotenv
load_dotenv()

from app.database import connect_database, parse_cost

# Environment
GOVT_VAT_RATE = (
    1 / float(os.getenv("GOVT_VAT_RATE_DIVISOR", "21"))
    if os.getenv("GOVT_VAT_RATE_DIVISOR")
    else 0.0
)
AMAZON_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.05"))
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "AED")

# SKUs to recalc
SKUS_TO_RECALCULATE = [
    "SDSQXAV-1T00-GN6MA",
    "HDWG740EZSTCU",
    "STKL2000404",
]


def recalculate_order_items_for_skus(sku_list: list):
    """
    Recalculate ONLY:
        - SSKU
        - Brand
        - Category
        - COG
        - Profit

    WITHOUT touching:
        - FeeIncl
        - FeePct
        - FBAFeesIncl
        - TotalFee
        - RVAT
        - VAT
    """

    conn = connect_database()
    cursor = conn.cursor()

    try:
        placeholders = ",".join("?" * len(sku_list))

        # 1. Fetch rows for these SKUs
        select_sql = f"""
            SELECT 
                AmazonOrderId,
                SKU,
                ASIN,
                Qty,
                UnitPrice,
                Currency
            FROM OrderItems
            WHERE SKU IN ({placeholders})
        """

        cursor.execute(select_sql, sku_list)
        rows = cursor.fetchall()

        print(f"Found {len(rows)} OrderItems rows to recalculate")

        if not rows:
            print("No rows found for the specified SKUs.")
            return

        # 2. Fetch product details for ASINs
        asin_list = list(set([row[2] for row in rows if row[2]]))
        product_details = {}

        if asin_list:
            asin_placeholders = ",".join("?" * len(asin_list))
            detail_sql = f"""
                SELECT
                    pm.asin,
                    ir.Cost,
                    ir.Brand,
                    ir.Category,
                    ir.ItemName
                FROM ProductMapping pm
                LEFT JOIN InventoryReport ir ON pm.ssku = ir.PartNumber
                WHERE pm.asin IN ({asin_placeholders})
            """
            cursor.execute(detail_sql, asin_list)
            for d in cursor.fetchall():
                product_details[d[0]] = {
                    "cost": d[1],
                    "brand": d[2],
                    "category": d[3],
                    "item_name": d[4],
                }

        # 3. Fetch SSKU mapping
        sku_to_ssku = {}
        mapping_sql = f"""
            SELECT sku, ssku FROM ProductMapping WHERE sku IN ({placeholders})
        """
        cursor.execute(mapping_sql, sku_list)
        for m in cursor.fetchall():
            sku_to_ssku[m[0]] = m[1]

        updated_count = 0

        # 4. Process each row
        for row in rows:
            order_id = row[0]
            sku = row[1]
            asin = row[2]
            qty = row[3] or 1
            unit_price = row[4]
            currency = row[5] or BASE_CURRENCY_CODE

            if not unit_price or unit_price <= 0:
                print(f"  Skipping {order_id}/{sku}: No valid unit price")
                continue

            # Get SSKU
            ssku = sku_to_ssku.get(sku, sku)

            # Get product details
            details = product_details.get(asin, {})
            brand = details.get("brand")
            category = details.get("category")

            # 5. Fetch existing fee fields (DO NOT REPLACE THEM)
            fee_sql = """
                SELECT FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT, VAT
                FROM OrderItems
                WHERE AmazonOrderId = ? AND SKU = ?
            """
            cursor.execute(fee_sql, (order_id, sku))
            fee_row = cursor.fetchone()

            if not fee_row:
                print(f"  Skipping {order_id}/{sku}: No existing fee data")
                continue

            fee_incl, fee_pct, fba_fees_incl, total_fee, rvat, vat = fee_row

            # 6. Compute COG
            cost = parse_cost(details.get("cost"))
            cog_total = cost * qty if cost is not None else None
            cog = -float(cog_total) if cog_total is not None else None

            # 7. Compute Profit using existing fees (convert all to float)
            subtotal_val_f = float(unit_price) * qty
            total_fee_f = float(total_fee) if total_fee is not None else None
            vat_f = float(vat) if vat is not None else None
            rvat_f = float(rvat) if rvat is not None else None
            cog_total_f = float(cog_total) if cog_total is not None else None

            if (
                subtotal_val_f is not None
                and total_fee_f is not None
                and vat_f is not None
                and rvat_f is not None
                and cog_total_f is not None
            ):
                profit = subtotal_val_f - (-total_fee_f) - (-vat_f) + rvat_f - cog_total_f
            else:
                profit = None

            # 8. Update only the required fields
            update_sql = """
                UPDATE OrderItems
                SET 
                    SSKU = ?,
                    Brand = ?,
                    Category = ?,
                    COG = ?,
                    Profit = ?
                WHERE AmazonOrderId = ? AND SKU = ?
            """

            cursor.execute(update_sql, (
                ssku,
                brand,
                category,
                cog,
                profit,
                order_id,
                sku,
            ))

            updated_count += cursor.rowcount
            print(f"  Updated {order_id}/{sku}: Profit={profit}")

        conn.commit()
        print(f"\n✅ Successfully updated {updated_count} rows")

    except Exception as e:
        conn.rollback()
        print(f"❌ Error: {e}")
        raise

    finally:
        cursor.close()
        conn.close()


def main():
    print("=" * 60)
    print("RECALCULATING ORDER ITEMS FOR NEW PRODUCT MAPPINGS")
    print("=" * 60)
    print(f"SKUs to process: {SKUS_TO_RECALCULATE}\n")

    recalculate_order_items_for_skus(SKUS_TO_RECALCULATE)

    print("\nDone!")


if __name__ == "__main__":
    main()
