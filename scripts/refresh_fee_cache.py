#!/usr/bin/env python3
import asyncio
import csv
import time
from io import StringIO

import config
from app.database import connect_database, parse_cost, get_product_details_by_asin
from app.utilities.fetch_report import fetch_spapi_report   # <-- unified fetcher
from app.utilities.utils import get_now_iso_string_with_custom_utc_offset
from app.fee_estimator import get_my_fee_estimate_batch   # <-- batch estimator


# ---------------------------------------------------------
# Fetch Active Listings Report (Unified Fetcher)
# ---------------------------------------------------------
async def fetch_active_listings_report():
    MAX_ATTEMPTS = 10

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}] "
                  f"[Active Listings] Attempt {attempt}/{MAX_ATTEMPTS}")

            # Use unified fetcher (raw text)
            text = fetch_spapi_report(
                report_type="GET_MERCHANT_LISTINGS_DATA",
                output_type="raw",
                params={"reportOptions": {"preferredReportDocumentLocale": "en_US"}}
            )

            return text

        except Exception as e:
            print(f"[{get_now_iso_string_with_custom_utc_offset()}] "
                  f"[Active Listings] Error: {e}")

            if attempt < MAX_ATTEMPTS:
                print(f"[{get_now_iso_string_with_custom_utc_offset()}] "
                      f"Waiting 60 seconds before retrying...")
                time.sleep(60)
            else:
                raise RuntimeError("Failed to retrieve Active Listings report after retries")


# ---------------------------------------------------------
# Main: refresh fee cache
# ---------------------------------------------------------
async def refresh_fee_cache():
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] === FeeEstimatesCache Refresh Started ===")

    listings_text = await fetch_active_listings_report()
    reader = csv.DictReader(StringIO(listings_text), delimiter="\t")

    active_items = []
    asin_list = set()

    for lr in reader:
        sku = (lr.get("seller-sku") or "").strip().upper()
        asin = (lr.get("asin1") or "").strip().upper()
        raw_price = lr.get("price")
        price = parse_cost(raw_price) if raw_price else None

        if sku and asin and price:
            active_items.append((sku, asin, float(price)))
            asin_list.add(asin)

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Loaded {len(active_items)} active SKUs")

    if not active_items:
        print(f"[{get_now_iso_string_with_custom_utc_offset()}] No active listings found — aborting.")
        return

    # ---------------------------------------------------------
    # Load product details (COG)
    # ---------------------------------------------------------
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Loading product details...")
    conn = connect_database()
    cursor = conn.cursor()
    product_details = get_product_details_by_asin(cursor, list(asin_list)) or {}
    cursor.close()
    conn.close()

    items_with_cog = []
    for sku, asin, price in active_items:
        d = product_details.get(asin) or {}
        cog = parse_cost(d.get("cost"))
        if cog is not None:
            items_with_cog.append((sku, asin, price, float(cog)))

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] {len(items_with_cog)} SKUs have valid COG")

    if not items_with_cog:
        print(f"[{get_now_iso_string_with_custom_utc_offset()}] No SKUs with COG — aborting.")
        return

    # ---------------------------------------------------------
    # Fee estimation (BATCH)
    # ---------------------------------------------------------
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Estimating fees for {len(items_with_cog)} SKUs...")

    batch_input = [
        {"sku": sku, "asin": asin, "price": price}
        for (sku, asin, price, _) in items_with_cog
    ]

    fee_results = get_my_fee_estimate_batch(batch_input)

    # ---------------------------------------------------------
    # Compute financials (NULL‑propagating)
    # ---------------------------------------------------------
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Computing financials...")

    cache_rows = []

    for (sku, asin, price, cog) in items_with_cog:
        key = (sku, asin, price)
        fr = fee_results.get(key) or {}

        ref = fr.get("referral")
        fba = fr.get("fba")

        # If either fee is None → all downstream values become None
        if ref is None or fba is None:
            charges = None
            vat = None
            net = None
            profit = None
        else:
            charges = ref + fba
            vat = price * config.GOVT_VAT_RATE
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

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Prepared {len(cache_rows)} rows for DB")

    # ---------------------------------------------------------
    # Save to DB
    # ---------------------------------------------------------
    print(f"[{get_now_iso_string_with_custom_utc_offset()}] Saving to FeeEstimatesCache...")

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

    print(f"[{get_now_iso_string_with_custom_utc_offset()}] === FeeEstimatesCache Refresh Complete ===")


if __name__ == "__main__":
    asyncio.run(refresh_fee_cache())