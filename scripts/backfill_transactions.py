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
# Environment
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


def parse_posted_date(value):
    """Convert ISO8601 Zulu string to Python datetime."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except:
        return None


def safe_decimal(value):
    """Convert any numeric-like value to float, else None."""
    try:
        return float(value)
    except:
        return None


# -------------------------------------------------------------------
# Backfill Logic
# -------------------------------------------------------------------

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

        # Fetch transactions
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

        # ---------------- DB UPSERT BLOCK ----------------
        try:
            conn = connect_database()
            cur = conn.cursor()

            # 1. Delete ALL rows with TransactionIds in this batch
            tids = [row["TransactionId"] for row in rows]
            placeholders = ",".join("?" for _ in tids)

            cur.execute(
                f"DELETE FROM spapi_app_user.FinancialTransactions WHERE TransactionId IN ({placeholders})",
                tids
            )

            # 2. Prepare batch insert values
            insert_values = []
            for row in rows:
                row["PostedDate"] = parse_posted_date(row["PostedDate"])

                insert_values.append((
                    row["TransactionId"],          # 1
                    row["PostedDate"],             # 2
                    row["TransactionType"],        # 3
                    row["TransactionStatus"],      # 4
                    row["AmazonOrderId"],          # 5
                    row["SellerSKU"],              # 6
                    row["ASIN"],                   # 7
                    row["SSKU"],                   # 8
                    row["QuantityShipped"],        # 9
                    safe_decimal(row["Principal"]),          # 10
                    safe_decimal(row["ShippingCharges"]),    # 11
                    safe_decimal(row["Promotions"]),         # 12
                    safe_decimal(row["FBAFees"]),            # 13
                    safe_decimal(row["Commission"]),         # 14
                    safe_decimal(row["FixedClosingFee"]),    # 15
                    safe_decimal(row["VariableClosingFee"]), # 16
                    safe_decimal(row["ShippingChargeback"]), # 17
                    safe_decimal(row["RefFee"]),             # 18
                    safe_decimal(row["Total"]),              # 19
                ))

            # 3. Batch insert
            cur.fast_executemany = True
            cur.executemany("""
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
                    Commission,
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
                    ?,?,?,?,?,?,?,?,?,
                    DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
                    DATEADD(HOUR,4,SYSDATETIMEOFFSET())
                )
            """, insert_values)

            conn.commit()
            print(f"Upserted {len(rows)} rows")
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


# -------------------------------------------------------------------
# Entry Point
# -------------------------------------------------------------------

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