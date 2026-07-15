"""
OrderItems Ingestion Pipeline
=============================

This module ingests Amazon FBA order data from the
`GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL` report (FBA version) and transforms it
into normalized rows for the `OrderItems` table.

The pipeline uses the Amazon Orders Report rather than the live `getOrders` /
`getOrderItems` API for several important reasons:

1. Pending Order Pricing
   - The Orders API does not reliably provide item pricing for pending orders.
   - The flat-file orders report includes price fields that are required for
     fee estimation, VAT calculations, and profitability reporting.
   - Since fee estimation depends on the item sale price, the report is a more
     suitable source for accounting preparation.

2. Bulk Order Fetching Performance
   - The report API is more efficient for retrieving a large number of orders
     over a time window.
   - Instead of making many paginated API calls to `getOrders` and then
     additional calls to `getOrderItems` per order, the report provides order
     item rows in bulk.
   - This significantly reduces ingestion time when processing many updated or
     newly created orders.

3. Rate Limit Practicality
   - The Orders API has stricter practical limitations for high-volume
     ingestion because order items often require separate API calls per order.
   - Using the report-based approach avoids excessive API calls and reduces the
     likelihood of throttling during regular ingestion jobs.

The report data is merged with internal product metadata, enriched with SKU to
SSKU mappings, ASIN-level product details, estimated Amazon fees, VAT, COG, and
profitability values. The resulting rows are prepared for insertion or update
in the `OrderItems` table with consistent accounting logic.

Important:
----------
Amazon order reports provide the order item price and quantity, but they do not
provide the final actual Amazon fees charged in settlement. For that reason,
this pipeline estimates referral and FBA fees using Amazon's Product Fees API.
Actual charged fees should be reconciled separately from financial events or
settlement reports.

Pipeline Overview
-----------------

1. Determine Report Window
   - Uses `LastUpdatedAfter` or `CreatedAfter` / `CreatedBefore` to compute the
     reporting range.
   - Falls back to the last 10 hours if no parameters are provided.

2. Fetch Orders Report
   - Downloads the TSV report from SP-API using the unified report fetcher.
   - Uses report type:
       `GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL`
   - Parses the downloaded report into a structured list of dictionaries.

3. Load Product Metadata
   - Loads SKU to SSKU mapping.
   - Loads ASIN-level product details such as brand, category, title, and COG.
   - These enrichments are required for normalized reporting, fee calculations,
     and profitability calculations.

4. Prepare Fee Estimation Inputs
   - Extracts unique `(SKU, ASIN, UnitPrice)` triples for purchasable items.
   - Unit price is calculated from the report as:
       `unit_price = line_total / quantity`
   - Only valid items with SKU, ASIN, positive quantity, and positive price are
     sent for fee estimation.

5. Fee Estimation
   - Calls `get_my_fee_estimate_batch()` once for all unique fee-estimation
     inputs.
   - Fee estimates are requested from Amazon's Product Fees API.
   - The estimator returns estimated referral fee and estimated FBA fee per
     unit.
   - Results are mapped back to each order item using `(SKU, ASIN, UnitPrice)`.

6. Financial Calculations
   For each order item, the following values are computed:

   • Subtotal
       `subtotal = unit_price * qty`

   • VAT
       `vat = -(subtotal * GOVT_VAT_RATE)`

   • COG / Cost of Goods
       `cog = -(product_cost * qty)`

     COG is stored as a negative value because accounting costs are represented
     as negative amounts.

   • Referral Fee Including VAT
       `ref_total = referral_per_unit * FEES_ESTIMATE_VAT_MULTIPLIER * qty`
       `FeeIncl = -ref_total`

   • FBA Fee Including VAT
       `fba_total = fba_per_unit * FEES_ESTIMATE_VAT_MULTIPLIER * qty`
       `FBAFeesIncl = -fba_total`

   • Total Fee
       `TotalFee = -(ref_total + fba_total)`

   • Fee Percentage
       `FeePct = (referral_per_unit / unit_price) * 100`

   • RVAT
       `rvat = (referral_per_unit + fba_per_unit)
               * (FEES_ESTIMATE_VAT_MULTIPLIER - 1)
               * qty`

     RVAT represents the VAT portion included inside the estimated Amazon fees.

   • Profit
       `profit = subtotal - total_fee - vat + rvat - cog`

   If any required component is missing, such as fee estimates, VAT, or COG,
   profit is set to `None`.

7. Build Normalized Output Rows
   Each output row includes:

       • Amazon order identifiers
       • Order date and last update date
       • SKU, ASIN, and SSKU
       • Brand, category, and title
       • Quantity, unit price, subtotal, and currency
       • Estimated referral fee, FBA fee, total fee, and fee percentage
       • VAT, RVAT, COG, and profit
       • Refund, return, reimbursement, and removal placeholder fields
       • Order status
       • FirstSeenAt and LastSeenAt timestamps

8. Return Final Rows
   - The function returns normalized, enriched, accounting-ready rows.
   - The caller is responsible for inserting or updating the `OrderItems` table.
   - This module does not directly persist rows to the database.

Summary
-------
This pipeline uses Amazon's bulk orders report as the primary source because it
is faster and more reliable for high-volume ingestion than repeatedly calling
the Orders API. It also provides pricing information needed for pending orders,
which is essential for fee estimation and profitability tracking.

The pipeline produces deterministic, auditable `OrderItems` rows by combining
Amazon report data, internal product metadata, estimated Amazon fees, VAT logic,
COG, and profit calculations into a consistent accounting-ready structure.
"""

import time
import csv
import io
from datetime import datetime, timedelta, timezone
from app.database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
    connect_database,
)
from app.fee_estimator import get_my_fee_estimate_batch
from app.utilities.utils import (
    to_utc_plus_offset_naive,
    now_utc_plus_offset_naive,
    convert_utc_to_utcz_string,
)
from app.utilities.fetch_report import fetch_spapi_report   # <-- unified fetcher
from config import (
    GOVT_VAT_RATE,
    BASE_CURRENCY_CODE,
    FEES_ESTIMATE_VAT_MULTIPLIER,
    MARKETPLACE_ID,
)

# -------------------------------------------------------------------
# Main orders logic (using unified fetcher)
# -------------------------------------------------------------------

async def get_orders_async(params):
    start_time = time.time()

    # ---------------------------------------------------------------
    # 1. Compute report window
    # ---------------------------------------------------------------
    last_updated_after = params.get("LastUpdatedAfter")
    created_after = params.get("CreatedAfter")
    created_before = params.get("CreatedBefore")

    if last_updated_after:
        end_dt = datetime.now(timezone.utc)
        try:
            start_dt = datetime.fromisoformat(last_updated_after.replace("Z", "+00:00"))
        except:
            start_dt = end_dt - timedelta(hours=10)

    elif created_after and created_before:
        start_dt = datetime.fromisoformat(created_after.replace("Z", "+00:00"))
        end_dt = datetime.fromisoformat(created_before.replace("Z", "+00:00"))

    else:
        end_dt = datetime.now(timezone.utc)
        start_dt = end_dt - timedelta(hours=10)

    print(f"Requesting report for {start_dt.isoformat()} to {end_dt.isoformat()}")

    # ---------------------------------------------------------------
    # 2. Fetch report using unified fetcher
    # ---------------------------------------------------------------
    decoded = fetch_spapi_report(
        report_type="GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
        output_type="raw",
        params={
            "dataStartTime": convert_utc_to_utcz_string(start_dt),
            "dataEndTime": convert_utc_to_utcz_string(end_dt),
            "marketplaceIds": [MARKETPLACE_ID],
        }
    )

    # ---------------------------------------------------------------
    # 3. Parse TSV
    # ---------------------------------------------------------------
    reader = csv.DictReader(io.StringIO(decoded), delimiter="\t")
    rows = list(reader)

    if not rows:
        print("No rows in report.")
        return []

    # ---------------------------------------------------------------
    # 4. Load product mapping + details
    # ---------------------------------------------------------------
    all_skus = list({r.get("sku") or r.get("SKU") for r in rows})
    asin_list = list({r.get("asin") or r.get("ASIN") for r in rows})

    conn = connect_database()
    cursor = conn.cursor()
    try:
        product_mapping = get_product_mapping(cursor, all_skus)
        product_details = get_product_details_by_asin(cursor, asin_list)
    finally:
        cursor.close()
        conn.close()

    # ---------------------------------------------------------------
    # 5. Prepare fee items
    # ---------------------------------------------------------------
    items_to_est = []
    report_items = []

    for r in rows:
        order_id = r.get("amazon-order-id") or r.get("AmazonOrderId")
        sku = r.get("sku") or r.get("SKU")
        asin = r.get("asin") or r.get("ASIN")

        qty_str = r.get("quantity") or r.get("Qty") or "1"
        try:
            qty = int(qty_str)
        except:
            qty = 1

        line_total_str = (
            r.get("item-price")
            or r.get("ItemPrice")
            or r.get("unit-price")
            or r.get("UnitPrice")
        )

        try:
            line_total = float(line_total_str)
        except:
            line_total = None

        unit_price = line_total / qty if (line_total and qty > 0) else None

        if sku and asin and unit_price and unit_price > 0:
            items_to_est.append((sku, asin, round(unit_price, 2)))

        report_items.append({
            "raw_row": r,
            "order_id": order_id,
            "sku": sku,
            "asin": asin,
            "qty": qty,
            "unit_price": unit_price,
            "line_total": line_total,
        })

    # ---------------------------------------------------------------
    # 6. Fee estimation (batch)
    # ---------------------------------------------------------------
    unique_items = list(set(items_to_est))
    print(f"[FEES] Unique items needing fees: {len(unique_items)}")

    batch_input = [
        {"sku": sku, "asin": asin, "price": price}
        for (sku, asin, price) in unique_items
    ]

    batch_results = get_my_fee_estimate_batch(batch_input)

    fees_by_key = {}
    stats = {"api_success": 0, "api_fail": 0}

    for (sku, asin, price) in unique_items:
        key = (sku, asin, price)
        result = batch_results.get(key)

        if not result:
            fees_by_key[key] = {"ReferralFee": None, "FBAFee": None}
            stats["api_fail"] += 1
            continue

        referral = result.get("referral")
        fba = result.get("fba")

        referral = None if referral is None else float(referral)
        fba = None if fba is None else float(fba)

        fees_by_key[key] = {"ReferralFee": referral, "FBAFee": fba}

        if referral is None and fba is None:
            stats["api_fail"] += 1
        else:
            stats["api_success"] += 1

    print("[FEES] Summary:")
    print(f"  API successes: {stats['api_success']}")
    print(f"  API failures:  {stats['api_fail']}")

    # ---------------------------------------------------------------
    # 7. Build output rows
    # ---------------------------------------------------------------
    output = []

    for item in report_items:
        r = item["raw_row"]
        order_id = item["order_id"]
        sku = item["sku"]
        asin = item["asin"]
        qty = item["qty"]
        unit_price = item["unit_price"]

        mapping = product_mapping.get(sku, {})
        prod_details = product_details.get(asin, {})

        ssku = mapping.get("ssku") if mapping else sku
        brand = prod_details.get("brand")
        category = prod_details.get("category")
        title = (
            prod_details.get("item_name")
            or prod_details.get("title")
            or r.get("product-name")
            or r.get("ProductName")
            or r.get("Title")
        )

        line_total = item["line_total"]
        subtotal = line_total

        fee_incl = None
        fee_pct = None
        fba_fees_incl = None
        total_fee = None
        rvat = None
        vat = None
        cog = None
        profit = None

        if sku and asin and unit_price and unit_price > 0 and qty > 0:
            key = (sku, asin, round(unit_price, 2))
            fee_block = fees_by_key.get(key) or {}

            referral_per_unit = fee_block.get("ReferralFee")
            fba_per_unit = fee_block.get("FBAFee")

            subtotal_val = unit_price * qty
            vat_total = subtotal_val * GOVT_VAT_RATE if subtotal_val is not None else None
            vat = -vat_total if vat_total is not None else None

            cost = parse_cost(prod_details.get("cost")) if prod_details else None
            cog_total = cost * qty if cost is not None else None
            cog = -cog_total if cog_total is not None else None

            if referral_per_unit is None or fba_per_unit is None:
                fee_incl = None
                fba_fees_incl = None
                total_fee = None
                fee_pct = None
                rvat = None
                profit = None

            else:
                ref_total = referral_per_unit * FEES_ESTIMATE_VAT_MULTIPLIER * qty
                fba_total = fba_per_unit * FEES_ESTIMATE_VAT_MULTIPLIER * qty
                total_fee_val = ref_total + fba_total

                fee_incl = -ref_total
                fba_fees_incl = -fba_total
                total_fee = -total_fee_val

                try:
                    fee_pct = (referral_per_unit / unit_price) * 100
                except:
                    fee_pct = None

                rvat_total = (
                    (referral_per_unit + fba_per_unit)
                    * (FEES_ESTIMATE_VAT_MULTIPLIER - 1.0)
                    * qty
                    if FEES_ESTIMATE_VAT_MULTIPLIER > 1.0
                    else 0.0
                )
                rvat = rvat_total if rvat_total is not None else None

                if (
                    subtotal_val is not None
                    and total_fee_val is not None
                    and vat_total is not None
                    and rvat_total is not None
                    and cog_total is not None
                ):
                    profit = subtotal_val - total_fee_val - vat_total + rvat_total - cog_total
                else:
                    profit = None

        currency = r.get("currency") or BASE_CURRENCY_CODE

        output.append({
            "AmazonOrderId": order_id,
            "OrderDate": to_utc_plus_offset_naive(r.get("purchase-date") or r.get("OrderDate")),
            "SKU": sku,
            "ASIN": asin,
            "SSKU": ssku,
            "Brand": brand,
            "Category": category,
            "Title": title,
            "Qty": qty,
            "UnitPrice": unit_price,
            "Subtotal": subtotal,
            "Currency": currency,
            "FeeIncl": fee_incl,
            "FeePct": fee_pct,
            "FBAFeesIncl": fba_fees_incl,
            "TotalFee": total_fee,
            "RVAT": rvat,
            "VAT": vat,
            "COG": cog,
            "Profit": profit,
            "Refund": None,
            "RefundDate": None,
            "ReturnDate": None,
            "ReturnDisposition": None,
            "ReturnReason": None,
            "LicensePlateNumber": None,
            "Reimbursed": None,
            "ReimbDate": None,
            "RemovalDate": None,
            "RemovalId": None,
            "RemovalTracking": None,
            "RemovalDelivery": None,
            "OrderStatus": r.get("order-status") or r.get("OrderStatus"),
            "LastUpdateDate": to_utc_plus_offset_naive(r.get("last-updated-date") or r.get("LastUpdateDate")),
            "FirstSeenAt": now_utc_plus_offset_naive(),
            "LastSeenAt": now_utc_plus_offset_naive(),
        })

    print(f"SUMMARY\nOrderItems rows: {len(output)}\nTime: {(time.time() - start_time) / 60:.1f}m")
    return output


async def get_orders(params):
    return await get_orders_async(params)