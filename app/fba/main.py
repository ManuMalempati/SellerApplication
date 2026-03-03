# RESPONSIBLE FOR FBAProductSummary Table
import time

from app.database import (
    connect_database,
    get_all_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    get_cached_fees,
)

from app.fba.database_fba import bulk_upsert_fba_data
from config import GOVT_VAT_RATE
from app.utilities.fetch_report import fetch_spapi_report
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
    # 2. Fetch FBA Inventory Report (TSV)
    # ---------------------------------------------------------
    print("Requesting FBA Inventory Report...")

    rows_tsv = fetch_spapi_report(
        report_type="GET_AFN_INVENTORY_DATA",
        output_type="tsv"
    )

    # ---------------------------------------------------------
    # 3. Aggregate by FNSKU
    # ---------------------------------------------------------
    fnsku_data = {}
    total_raw_rows = 0

    for line in rows_tsv:
        total_raw_rows += 1

        sku = (line.get("seller-sku") or "").strip()
        asin = (line.get("asin") or "").strip()
        qty = (line.get("Quantity Available") or "").strip()
        fnsku = (line.get("fulfillment-channel-sku") or "").strip()
        condition = (line.get("Warehouse-Condition-code") or "").strip()

        if not fnsku:
            continue

        qty_int = int(qty) if qty.isdigit() else 0

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

        if condition == "SELLABLE":
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
        r["COG"] = d.get("cost")
        r["Brand"] = d.get("brand")
        r["Category"] = d.get("category")

    # ---------------------------------------------------------
    # 5. Fetch L30 sales & traffic data
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
    # 6. Fetch Active Listings (TSV)
    # ---------------------------------------------------------
    print("Requesting Active Listings report to enrich price/title...")

    listings_tsv = fetch_spapi_report(
        report_type="GET_MERCHANT_LISTINGS_DATA",
        output_type="tsv",
        params={"reportOptions": {"preferredReportDocumentLocale": "en_US"}}
    )

    listings_map = {}
    for lr in listings_tsv:
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
            r["Title"] = listing.get("title")
            r["Sale-Price"] = listing.get("price")
        else:
            r["Title"] = None
            r["Sale-Price"] = None

    # ---------------------------------------------------------
    # 7. Filter to ACTIVE SKUs only
    # ---------------------------------------------------------
    rows = [r for r in rows if r["SKU"] in listings_map]
    print(f"Filtered to {len(rows)} ACTIVE SKUs")

    # ---------------------------------------------------------
    # 8. Fee estimation (cached)
    # ---------------------------------------------------------
    fee_items = [(r["SKU"], r["ASIN"], r["Sale-Price"]) for r in rows if r.get("Sale-Price")]

    print(f"[DEBUG] Fetching cached fees for {len(fee_items)} items...")

    conn = connect_database()
    cursor = conn.cursor()
    cached_fees = get_cached_fees(cursor, fee_items)
    cursor.close()
    conn.close()

    print("[DEBUG] Cached fee lookup complete.")

    # ---------------------------------------------------------
    # 9. Apply cached fees
    # ---------------------------------------------------------
    for r in rows:
        price = r.get("Sale-Price")
        asin = r.get("ASIN")
        sku = r.get("SKU")

        if not price or not asin or not sku:
            continue

        key = (sku, asin, price)
        f = cached_fees.get(key)

        if not f:
            r["Charges"] = None
            r["Est-VAT"] = None
            r["Est-Net"] = None
            r["Profit"] = None
            continue

        charges = f.get("Charges") or 0
        vat = price * GOVT_VAT_RATE
        est_net = price - charges - vat

        cog = parse_cost(r.get("COG"))
        profit = est_net - (cog or 0) if cog is not None else None

        r["Charges"] = charges
        r["Est-VAT"] = vat
        r["Est-Net"] = est_net
        r["Profit"] = profit

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
    print(f"Items with charges: {len([r for r in rows if r.get('Charges') is not None])}")
    print(f"Items with L30 data: {len([r for r in rows if r.get('TotalOrderItems_L30') is not None])}")
    print(f"Total time: {elapsed:.1f}s")
    print("=" * 60)

    return rows