import time

from app.database import (
    connect_database,
    get_all_product_mapping,
    get_product_details_by_asin,
)

from app.utilities.fetch_report import fetch_spapi_report


async def fba_unsuppressed_manage_inventory_report():
    """
    Uses:
        GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA

    Returns a normalized list of inventory rows.

    No database writes.
    No fee calculations.
    No sales traffic enrichment.

    Intended for inspection and future development.

    This is similar to GET_AFN_INVENTORY_DATA but client has advised to try this for better accuracy.
    """

    start_time = time.time()

    # ---------------------------------------------------------
    # 1. Load product mapping
    # ---------------------------------------------------------
    print("Loading product mapping...")

    conn = connect_database()
    cursor = conn.cursor()

    try:
        product_mappings = get_all_product_mapping(cursor) or {}
    finally:
        cursor.close()
        conn.close()

    # ---------------------------------------------------------
    # 2. Fetch report
    # ---------------------------------------------------------
    print("Requesting FBA Manage Inventory report...")

    rows_tsv = fetch_spapi_report(
        report_type="GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA",
        output_type="tsv"
    )

    print(f"Fetched {len(rows_tsv)} raw rows")

    # ---------------------------------------------------------
    # 3. Build output rows
    # ---------------------------------------------------------
    rows = []

    asins = set()

    for line in rows_tsv:

        sku = (line.get("sku") or "").strip()
        fnsku = (line.get("fnsku") or "").strip()
        asin = (line.get("asin") or "").strip()

        if asin:
            asins.add(asin)

        mapping = product_mappings.get(sku) or {}
        ssku = mapping.get("ssku")

        row = {
            "SKU": sku,
            "SSKU": str(ssku).strip() if ssku is not None else None,
            "ASIN": asin,
            "FNSKU": fnsku,

            "Title": line.get("product-name"),
            "Condition": line.get("condition"),

            "Sale-Price": line.get("your-price"),

            "MFN-Listing-Exists": line.get("mfn-listing-exists"),
            "MFN-Fulfillable-Qty": line.get("mfn-fulfillable-quantity"),

            "AFN-Listing-Exists": line.get("afn-listing-exists"),
            "AFN-Warehouse-Qty": line.get("afn-warehouse-quantity"),
            "AFN-Fulfillable-Qty": line.get("afn-fulfillable-quantity"),
            "AFN-Unsellable-Qty": line.get("afn-unsellable-quantity"),
            "AFN-Reserved-Qty": line.get("afn-reserved-quantity"),
            "AFN-Total-Qty": line.get("afn-total-quantity"),

            "Per-Unit-Volume": line.get("per-unit-volume"),

            "AFN-Inbound-Working-Qty": line.get("afn-inbound-working-quantity"),
            "AFN-Inbound-Shipped-Qty": line.get("afn-inbound-shipped-quantity"),
            "AFN-Inbound-Receiving-Qty": line.get("afn-inbound-receiving-quantity"),

            "AFN-Researching-Qty": line.get("afn-researching-quantity"),

            "AFN-Reserved-Future-Supply": line.get("afn-reserved-future-supply"),
            "AFN-Future-Supply-Buyable": line.get("afn-future-supply-buyable"),
        }

        rows.append(row)

    # ---------------------------------------------------------
    # 4. Load product details
    # ---------------------------------------------------------
    print(f"Loading product details for {len(asins)} ASINs...")

    conn = connect_database()
    cursor = conn.cursor()

    try:
        product_details = get_product_details_by_asin(
            cursor,
            list(asins)
        ) or {}
    finally:
        cursor.close()
        conn.close()

    for row in rows:
        details = product_details.get(row["ASIN"]) or {}

        row["COG"] = details.get("cost")
        row["Brand"] = details.get("brand")
        row["Category"] = details.get("category")

    # ---------------------------------------------------------
    # 5. Summary
    # ---------------------------------------------------------
    elapsed = time.time() - start_time

    print("=" * 60)
    print("FBA MANAGE INVENTORY REPORT")
    print("=" * 60)
    print(f"Rows: {len(rows)}")
    print(f"Unique ASINs: {len(asins)}")
    print(f"Time: {elapsed:.1f}s")
    print("=" * 60)

    return rows