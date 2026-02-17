import time
import csv
from io import StringIO

from ..database import (
    connect_database,
    get_all_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    bulk_upsert_fba_data,
)
from .config import GOVT_VAT_RATE
from .helpers import request_report, wait_for_report, download_report
from .pricing import run_pricing_batch
from .fees import run_fees_batch
from .sales_traffic import fetch_l30_sales_traffic


async def fba_report(save_to_db=True):
    start_time = time.time()

    # ---------------------------------------------------------
    # 1. Load product mapping
    # ---------------------------------------------------------
    print("Loading product mapping...")
    conn = connect_database()
    cursor = conn.cursor()
    product_mappings = get_all_product_mapping(cursor) or {}
    cursor.close()
    conn.close()

    # ---------------------------------------------------------
    # 2. Request and download FBA inventory report
    # ---------------------------------------------------------
    report_id = request_report("GET_AFN_INVENTORY_DATA")
    print(f"Report requested: {report_id}")

    if not report_id:
        raise RuntimeError("Failed to request report: no report_id returned")

    document_id = wait_for_report(report_id)
    print(f"Report ready: {document_id}")

    raw_text = download_report(document_id)

    # ---------------------------------------------------------
    # 3. Parse inventory report and aggregate by FNSKU
    # ---------------------------------------------------------
    fnsku_data = {}  # {fnsku: {base_data, sellable_qty, unsellable_qty}}
    reader = csv.DictReader(StringIO(raw_text), delimiter="\t")
    total_raw_rows = 0

    for line in reader:
        total_raw_rows += 1
        sku = (line.get("seller-sku") or "").strip()
        asin = (line.get("asin") or "").strip()
        qty = (line.get("Quantity Available") or "").strip()
        fnsku = (line.get("fulfillment-channel-sku") or "").strip()
        warehouse_condition = (line.get("Warehouse-Condition-code") or "").strip()

        if not fnsku:
            continue

        qty_int = int(qty) if qty and qty.isdigit() else 0

        # Get SSKU from ProductMapping if exists, otherwise None
        ssku = (product_mappings.get(sku) or {}).get("ssku")
        if ssku is not None:
            ssku = str(ssku).strip()

        if fnsku not in fnsku_data:
            fnsku_data[fnsku] = {
                "SKU": sku,
                "ASIN": asin,
                "SSKU": ssku,
                "FNSKU": fnsku,
                "Sellable-Qty": 0,
                "Unsellable-Qty": 0,
            }

        if warehouse_condition == "SELLABLE":
            fnsku_data[fnsku]["Sellable-Qty"] += qty_int
        else:
            fnsku_data[fnsku]["Unsellable-Qty"] += qty_int

    rows = []
    for fnsku, data in fnsku_data.items():
        data["FBA-Stock"] = data["Sellable-Qty"] + data["Unsellable-Qty"]
        rows.append(data)

    print(f"Raw AFN report rows (unfiltered): {total_raw_rows}")
    print(f"Parsed {len(rows)} unique FNSKUs")

    # ---------------------------------------------------------
    # 4. Load product details
    # ---------------------------------------------------------
    asins = list({r["ASIN"] for r in rows if r["ASIN"]})
    print(f"Loading product details for {len(asins)} ASINs...")

    conn = connect_database()
    cursor = conn.cursor()
    product_details = get_product_details_by_asin(cursor, asins) or {}
    cursor.close()
    conn.close()

    for r in rows:
        d = product_details.get(r["ASIN"]) or {}
        r["Title"] = d.get("item_name")
        r["COG"] = d.get("cost")
        r["Brand"] = d.get("brand")
        r["Category"] = d.get("category")

    # ---------------------------------------------------------
    # 5. Load Reserved Inventory from CurrentInventory
    # ---------------------------------------------------------
    # ...reserved inventory logic removed...

    # ---------------------------------------------------------
    # 6. Fetch titles from Listings API (disabled)
    # ---------------------------------------------------------
    print("Skipping Listings API title fetching (disabled). Titles from DB/Inventory remain unchanged.")

    # ---------------------------------------------------------
    # 7. Fetch L30 sales & traffic data
    # ---------------------------------------------------------
    l30_data = fetch_l30_sales_traffic()

    for r in rows:
        asin = r["ASIN"]
        l30 = l30_data.get(asin, {})
        r["TotalOrderItems_L30"] = l30.get("TotalOrderItems_L30")
        r["OrderedProductSales_L30"] = l30.get("OrderedProductSales_L30")
        r["UnitsRefunded_L30"] = l30.get("UnitsRefunded_L30")
        r["BuyBoxPercentage_L30"] = l30.get("BuyBoxPercentage_L30")

    # ---------------------------------------------------------
    # 8. Fetch pricing
    # ---------------------------------------------------------
    skus = [r["SKU"] for r in rows]
    pricing = await run_pricing_batch(skus)

    fee_items = []
    for r in rows:
        price = pricing.get(r["SKU"])
        r["Sale-Price"] = price
        if price and r["ASIN"]:
            fee_items.append((r["SKU"], r["ASIN"], price))

    # ---------------------------------------------------------
    # 9. Estimate fees
    # ---------------------------------------------------------
    fees = await run_fees_batch(fee_items)

    for r in rows:
        sku = r["SKU"]
        asin = r["ASIN"]
        price = r["Sale-Price"]

        if price and asin:
            f = fees.get((sku, asin, price)) or {}
            net = f.get("net") or {}

            ref = float(net.get("ReferralFees", 0) or 0)
            fba = float(net.get("FBAFees", 0) or 0)
            vat = price * GOVT_VAT_RATE
            cog = parse_cost(r["COG"]) or 0

            r["Est-Fee"] = -ref if ref else None
            r["Est-FBA Fee"] = -fba if fba else None
            r["Est-VAT"] = -vat
            r["Est-Net"] = price - ref - fba - vat - cog
        else:
            r["Est-Fee"] = None
            r["Est-FBA Fee"] = None
            r["Est-VAT"] = None
            r["Est-Net"] = None

    # ---------------------------------------------------------
    # 10. Save to database (if requested)
    # ---------------------------------------------------------
    if save_to_db:
        print("Saving FBA data to ProductMapping table...")
        conn = connect_database()
        cursor = conn.cursor()
        try:
            success_count = bulk_upsert_fba_data(cursor, rows)
            conn.commit()
            print(f"Successfully saved {success_count}/{len(rows)} rows to database")
        except Exception as e:
            conn.rollback()
            print(f"Error saving to database: {e}")
            raise
        finally:
            cursor.close()
            conn.close()

    # ---------------------------------------------------------
    # 11. Summary
    # ---------------------------------------------------------
    elapsed = time.time() - start_time

    print("=" * 60)
    print("FBA REPORT - SUMMARY")
    print("=" * 60)
    print(f"Total items: {len(rows)}")
    print(f"Items with price: {len([r for r in rows if r['Sale-Price']])}")
    print(f"Items with fees: {len([r for r in rows if r['Est-Fee'] is not None])}")
    print(f"Items with L30 data: {len([r for r in rows if r.get('TotalOrderItems_L30') is not None])}")
    print(f"Total time: {elapsed:.1f}s")
    print("=" * 60)

    return rows
