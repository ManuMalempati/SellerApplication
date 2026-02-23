#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database


# -------------------------------------------------------------------
# Environment (from config)
# -------------------------------------------------------------------

BACKFILL_CHUNK_DAYS = config.BACKFILL_CHUNK_DAYS
SYNC_OVERLAP_HOURS = config.SYNC_OVERLAP_HOURS


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def fmt(dt: datetime) -> str:
    """Format datetime as ISO8601 Zulu."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


# -------------------------------------------------------------------
# Backfill Logic
# -------------------------------------------------------------------

async def run_backfill(start_date: datetime, end_date: datetime):
    """
    Backfills transactions from start_date to end_date in windows of BACKFILL_CHUNK_DAYS.
    """

    print("Backfill starting")
    print(f"Start: {fmt(start_date)}")
    print(f"End:   {fmt(end_date)}")
    print(f"Window size: {BACKFILL_CHUNK_DAYS} days")

    window_start = start_date
    total_upserted = 0
    window_index = 0

    while window_start < end_date:
        window_end = min(window_start + timedelta(days=BACKFILL_CHUNK_DAYS), end_date)
        window_index += 1

        params = {
            "postedAfter": fmt(window_start),
            "postedBefore": fmt(window_end),
        }

        print("\n------------------------------------------------------------")
        print(f"Window {window_index}: {params['postedAfter']} -> {params['postedBefore']}")
        print("Fetching...")

        # Direct call — retry logic already inside get_transactions()
        conn = connect_database()
        cur = conn.cursor()
        try:
            rows = get_transactions(params=params, db_cursor=cur)
        finally:
            cur.close()
            conn.close()

        print(f"Fetched {len(rows)} rows")

        # ---------------- DB UPSERT BLOCK ----------------
        try:
            conn = connect_database()
            cur = conn.cursor()

            upserted = 0
            for row in rows:
                tid = row["TransactionId"]

                # Delete existing row
                cur.execute(
                    "DELETE FROM spapi_app_user.FinancialTransactions WHERE TransactionId = ?",
                    (tid,)
                )

                # Insert new row
                cur.execute("""
                    INSERT INTO spapi_app_user.FinancialTransactions (
                        TransactionId,
                        PostedDate,
                        TransactionType,
                        TransactionStatus,
                        AmazonOrderId,
                        SKU,
                        ASIN,
                        SSKU,
                        Brand,
                        Category,
                        Currency,
                        SOLD,
                        ShippingCharge,
                        TotalPromotions,
                        SalesProceed,
                        Fee,
                        FBAFees,
                        ShippingChargeback,
                        TotalAmazonFees,
                        VAT,
                        R_VAT,
                        FeePercent,
                        COG,
                        NetProfit,
                        CreatedAt,
                        UpdatedAt
                    )
                    VALUES (
                        ?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,
                        DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
                        DATEADD(HOUR,4,SYSDATETIMEOFFSET())
                    )
                """, (
                    row["TransactionId"],
                    row["PostedDate"],
                    row["TransactionType"],
                    row["TransactionStatus"],
                    row["AmazonOrderId"],
                    row["SKU"],
                    row["ASIN"],
                    row["SSKU"],
                    row["Brand"],
                    row["Category"],
                    row["Currency"],
                    row["SOLD"],
                    row["ShippingCharge"],
                    row["TotalPromotions"],
                    row["SalesProceed"],
                    row["Fee"],
                    row["FBAFees"],
                    row["ShippingChargeback"],
                    row["TotalAmazonFees"],
                    row["VAT"],
                    row["R.VAT"],
                    row["Fee%"],
                    row["COG"],
                    row["Net Profit"],
                ))

                upserted += 1

            conn.commit()
            cur.close()
            conn.close()

            print(f"Upserted {upserted} rows")
            total_upserted += upserted

        except Exception as exc:
            print(f"Database error: {exc}")

        window_start = window_end

    print("\n============================================================")
    print(f"Backfill complete. Total rows upserted: {total_upserted}")
    print("============================================================")


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

def main():
    """
    Backfill from N days ago until now, with overlap.
    """

    now = datetime.now(timezone.utc)
    end_date = now - timedelta(hours=SYNC_OVERLAP_HOURS)

    days_back = int(os.getenv("BACKFILL_DAYS", "56"))
    start_date = end_date - timedelta(days=days_back)

    start = time.time()
    asyncio.run(run_backfill(start_date, end_date))
    elapsed = time.time() - start

    print(f"\nFinished in {elapsed:.1f} seconds")


if __name__ == "__main__":
    main()