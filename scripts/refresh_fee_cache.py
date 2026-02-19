#!/usr/bin/env python3
import asyncio
import csv
import time
from io import StringIO

from . import config
config.load_env()

from app.database import connect_database, parse_cost, get_product_details_by_asin
from app.fba.helpers import request_report, wait_for_report, download_report
from app.fba.fees import run_fees_batch
from app.fba.config import GOVT_VAT_RATE


# ---------------------------------------------------------
# Helper: request + wait + download with retry (60s cooldown)
# ---------------------------------------------------------
async def fetch_active_listings_report():
    MAX_ATTEMPTS = 10

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"[Active Listings] Attempt {attempt}/{MAX_ATTEMPTS}")

            report_id = request_report(
                "GET_MERCHANT_LISTINGS_DATA",
                params={"reportOptions": {"preferredReportDocumentLocale": "en_US"}}
            )

            if not report_id:
                raise RuntimeError("No reportId returned")

            doc_id = wait_for_report(report_id)
            text = download_report(doc_id)
            return text

        except Exception as e:
            print(f"[Active Listings] Error: {e}")
            if attempt < MAX_ATTEMPTS:
                print("Waiting 60 seconds before retrying...")
                time.sleep(60)
            else:
                raise RuntimeError("Failed to retrieve Active Listings report after retries")


# ---------------------------------------------------------
# Main: refresh fee cache
# ---------------------------------------------------------
async def refresh_fee_cache():
    print("Refreshing FeeEstimatesCache...")

    # ---------------------------------------------------------
    # 1. Load Active Listings (SKU, ASIN, Price)
    # ---------------------------------------------------------
    listings_text = await fetch_active_listings_report()

    reader = csv.DictReader(StringIO(listings_text), delimiter="\t")

    active_items = []  # (sku, asin, price)
    asin_list = set()

    for lr in reader:
        sku = (lr.get("seller-sku") or "").strip()
        asin = (lr.get("asin1") or "").strip()
        raw_price = lr.get("price")
        price = parse_cost(raw_price) if raw_price else None

        if sku and asin and price:
            active_items.append((sku, asin, price))
            asin_list.add(asin)

    print(f"Loaded {len(active_items)} active SKUs from listings")

    if not active_items:
        print("No active listings found — aborting cache refresh.")
        return

    # ---------------------------------------------------------
    # 2. Load product details (to get COG)
    # ---------------------------------------------------------
    print("Loading product details for COG...")
    conn = connect_database()
    cursor = conn.cursor()
    product_details = get_product_details_by_asin(cursor, list(asin_list)) or {}
    cursor.close()
    conn.close()

    # ---------------------------------------------------------
    # 3. Filter only items with valid COG
    # ---------------------------------------------------------
    items_with_cog = []
    for sku, asin, price in active_items:
        d = product_details.get(asin) or {}
        cog = parse_cost(d.get("cost"))

        if cog is not None:
            items_with_cog.append((sku, asin, price, cog))

    print(f"{len(items_with_cog)} SKUs have valid COG (out of {len(active_items)})")

    if not items_with_cog:
        print("No SKUs with COG found — aborting cache refresh.")
        return

    # ---------------------------------------------------------
    # 4. Call fee API in batch
    # ---------------------------------------------------------
    fee_items = [(sku, asin, price) for (sku, asin, price, _) in items_with_cog]
    fees = await run_fees_batch(fee_items)

    # ---------------------------------------------------------
    # 5. Compute Charges, VAT, Net, Profit
    # ---------------------------------------------------------
    cache_rows = []
    for (sku, asin, price, cog) in items_with_cog:
        resp = fees.get((sku, asin, price)) or {}
        net_block = resp.get("net") or {}

        ref = float(net_block.get("ReferralFees", 0) or 0)
        fba = float(net_block.get("FBAFees", 0) or 0)

        charges = ref + fba
        vat = price * GOVT_VAT_RATE
        net = price - charges - vat
        profit = net - cog

        cache_rows.append((
            sku,
            asin,
            price,
            ref,
            fba,
            charges,
            vat,
            net,
            cog,
            profit
        ))

    print(f"Prepared {len(cache_rows)} cache rows")

    # ---------------------------------------------------------
    # 6. Upsert into FeeEstimatesCache
    # ---------------------------------------------------------
    conn = connect_database()
    cursor = conn.cursor()
    cursor.fast_executemany = True

    sql = """
        MERGE FeeEstimatesCache AS target
        USING (VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)) AS src
              (SKU, ASIN, Price, ReferralFee, FBAFee, Charges, VAT, Net, COG, Profit)
        ON target.SKU = src.SKU AND target.ASIN = src.ASIN
        WHEN MATCHED THEN
            UPDATE SET
                Price = src.Price,
                ReferralFee = src.ReferralFee,
                FBAFee = src.FBAFee,
                Charges = src.Charges,
                VAT = src.VAT,
                Net = src.Net,
                COG = src.COG,
                Profit = src.Profit,
                CachedAt = GETDATE()
        WHEN NOT MATCHED THEN
            INSERT (SKU, ASIN, Price, ReferralFee, FBAFee, Charges, VAT, Net, COG, Profit)
            VALUES (src.SKU, src.ASIN, src.Price, src.ReferralFee, src.FBAFee, src.Charges, src.VAT, src.Net, src.COG, src.Profit);
    """

    cursor.executemany(sql, cache_rows)
    conn.commit()

    cursor.close()
    conn.close()

    print("FeeEstimatesCache refresh complete.")

if __name__ == "__main__":
    asyncio.run(refresh_fee_cache())