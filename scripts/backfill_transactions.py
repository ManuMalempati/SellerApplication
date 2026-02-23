#!/usr/bin/env python3
import os
import time
import asyncio
from datetime import datetime, timedelta, timezone

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database


BACKFILL_CHUNK_DAYS = config.BACKFILL_CHUNK_DAYS
SYNC_OVERLAP_HOURS = config.SYNC_OVERLAP_HOURS


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def fmt(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def parse_posted_date(value):
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except:
        return None


def safe_decimal(value):
    try:
        return float(value)
    except:
        return None


# ---------------------------------------------------------
# BACKFILL
# ---------------------------------------------------------

async def run_backfill(start_date: datetime, end_date: datetime):
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

        conn = connect_database()
        cur = conn.cursor()
        try:
            rows = get_transactions(params=params, db_cursor=cur)
        finally:
            cur.close()
            conn.close()

        print(f"Fetched {len(rows)} rows")

        if not rows:
            window_start = window_end
            continue

        try:
            conn = connect_database()
            cur = conn.cursor()

            for row in rows:
                amazon_order_id = row["AmazonOrderId"]
                sku = row["SellerSKU"]
                status = row["TransactionStatus"]

                # ---------------------------------------------------------
                # LIFECYCLE REPLACEMENT LOGIC
                # ---------------------------------------------------------

                if status == "DEFERRED":
                    cur.execute("""
                        DELETE FROM spapi_app_user.FinancialTransactions
                        WHERE AmazonOrderId = ?
                          AND SellerSKU = ?
                          AND TransactionStatus = 'DEFERRED'
                    """, (amazon_order_id, sku))

                elif status == "DEFERRED_RELEASED":
                    cur.execute("""
                        DELETE FROM spapi_app_user.FinancialTransactions
                        WHERE AmazonOrderId = ?
                          AND SellerSKU = ?
                          AND TransactionStatus IN ('DEFERRED', 'DEFERRED_RELEASED')
                    """, (amazon_order_id, sku))

                elif status == "RELEASED":
                    cur.execute("""
                        DELETE FROM spapi_app_user.FinancialTransactions
                        WHERE AmazonOrderId = ?
                          AND SellerSKU = ?
                          AND TransactionStatus IN ('DEFERRED', 'DEFERRED_RELEASED')
                    """, (amazon_order_id, sku))

                # ---------------------------------------------------------
                # INSERT NEW ROW
                # ---------------------------------------------------------

                row["PostedDate"] = parse_posted_date(row["PostedDate"])

                cur.execute("""
                    INSERT INTO spapi_app_user.FinancialTransactions (
                        TransactionId,
                        PostedDate,
                        TransactionType,
                        TransactionStatus,
                        AmazonOrderId,
                        SellerSKU,
                        ASIN,
                        SSKU,
                        QuantityShipped,
                        Principal,
                        ShippingCharges,
                        Promotions,
                        FBAFees,
                        FixedClosingFee,
                        VariableClosingFee,
                        ShippingChargeback,
                        RefFee,
                        Total,
                        CreatedAt,
                        UpdatedAt
                    )
                    VALUES (
                        ?,?,?,?,?,?,?,?,?,?,
                        ?,?,?,?,?,?,?,?,
                        DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
                        DATEADD(HOUR,4,SYSDATETIMEOFFSET())
                    )
                """, (
                    row["TransactionId"],
                    row["PostedDate"],
                    row["TransactionType"],
                    row["TransactionStatus"],
                    row["AmazonOrderId"],
                    row["SellerSKU"],
                    row["ASIN"],
                    row["SSKU"],
                    row["QuantityShipped"],
                    safe_decimal(row["Principal"]),
                    safe_decimal(row["ShippingCharges"]),
                    safe_decimal(row["Promotions"]),
                    safe_decimal(row["FBAFees"]),
                    safe_decimal(row["FixedClosingFee"]),
                    safe_decimal(row["VariableClosingFee"]),
                    safe_decimal(row["ShippingChargeback"]),
                    safe_decimal(row["RefFee"]),
                    safe_decimal(row["Total"]),
                ))

            conn.commit()
            total_upserted += len(rows)

        except Exception as exc:
            conn.rollback()
            print("Database error:", exc)
        finally:
            cur.close()
            conn.close()

        window_start = window_end

    print("\n============================================================")
    print(f"Backfill complete. Total rows upserted: {total_upserted}")
    print("============================================================")


# ---------------------------------------------------------
# Entry Point
# ---------------------------------------------------------

def main():
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
