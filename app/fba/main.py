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
    fnsku_data = {}
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
    # 5. Reserved Inventory (skipped)
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
    # 8. Enrich with Active Listings report
    # ---------------------------------------------------------
    print("Requesting Active Listings report to enrich price/title...")
    report_id = request_report("GET_MERCHANT_LISTINGS_DATA", params={
        "reportOptions": {"preferredReportDocumentLocale": "en_US"}
    })
    if not report_id:
        raise RuntimeError("Failed to request Active Listings report")

    doc_id = wait_for_report(report_id)
    print(f"Active Listings document ready: {doc_id}")

    listings_text = download_report(doc_id)
    reader = csv.DictReader(StringIO(listings_text), delimiter="\t")

    listings_map = {}
    for lr in reader:
        sku = (lr.get("seller-sku") or "").strip()
        if not sku:
            continue
        title = lr.get("item-name") or None
        raw_price = lr.get("price")
        price = parse_cost(raw_price) if raw_price else None
        listings_map[sku] = {"title": title, "price": price}

    print(f"Loaded {len(listings_map)} active listings for enrichment")

    for r in rows:
        sku = r["SKU"]
        listing = listings_map.get(sku)
        if listing:
            if listing.get("title"):
                r["Title"] = listing.get("title")
            r["Sale-Price"] = listing.get("price")
        else:
            r["Sale-Price"] = r.get("Sale-Price") if "Sale-Price" in r else None

    # ---------------------------------------------------------
    # 9. Estimate fees (SKIP invalid rows)
    # ---------------------------------------------------------
    fee_items = []
    for r in rows:
        price = r.get("Sale-Price")
        asin = r.get("ASIN")
        sku = r.get("SKU")
        cog = parse_cost(r.get("COG"))

        # Skip if any required field is missing
        if not price or not asin or not sku or cog is None:
            r["Est-Fee"] = None
            r["Est-FBA Fee"] = None
            r["Est-VAT"] = None
            r["Est-Net"] = None
            continue

        fee_items.append((sku, asin, price))

    fees = await run_fees_batch(fee_items)

    for r in rows:
        price = r.get("Sale-Price")
        asin = r.get("ASIN")
        sku = r.get("SKU")
        cog = parse_cost(r.get("COG"))

        # Skip if missing required fields
        if not price or not asin or not sku or cog is None:
            continue

        f = fees.get((sku, asin, price)) or {}
        net = f.get("net") or {}

        ref = float(net.get("ReferralFees", 0) or 0)
        fba = float(net.get("FBAFees", 0) or 0)
        vat = price * GOVT_VAT_RATE

        r["Est-Fee"] = -ref if ref else None
        r["Est-FBA Fee"] = -fba if fba else None
        r["Est-VAT"] = -vat
        r["Est-Net"] = price - ref - fba - vat - cog

    # ---------------------------------------------------------
    # 10. Save to database
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
    print(f"Items with price: {len([r for r in rows if r.get('Sale-Price')])}")
    print(f"Items with fees: {len([r for r in rows if r['Est-Fee'] is not None])}")
    print(f"Items with L30 data: {len([r for r in rows if r.get('TotalOrderItems_L30') is not None])}")
    print(f"Total time: {elapsed:.1f}s")
    print("=" * 60)

    return rows
