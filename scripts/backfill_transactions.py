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


# ---------------------------------------------------------
# Backfill Logic
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
            conn.autocommit = False

            # -------------------------------------------------
            # 1. Deduplicate in Python by TransactionId
            # -------------------------------------------------
            unique = {}
            for r in rows:
                tid = r.get("TransactionId")
                if tid:
                    unique[tid] = r
            rows = list(unique.values())
            print(f"Window {window_index} after dedup: {len(rows)}")

            if not rows:
                conn.commit()
                window_start = window_end
                continue

            # -------------------------------------------------
            # 2. Create temp table
            # -------------------------------------------------
            cur.execute("""
            IF OBJECT_ID('tempdb..#TempFinancial') IS NOT NULL DROP TABLE #TempFinancial;

            CREATE TABLE #TempFinancial(
                TransactionId NVARCHAR(100) PRIMARY KEY,
                PostedDate DATETIMEOFFSET,
                TransactionType NVARCHAR(50),
                TransactionStatus NVARCHAR(50),
                AmazonOrderId NVARCHAR(50),
                SellerSKU NVARCHAR(100),
                ASIN NVARCHAR(50),
                SSKU NVARCHAR(50),
                QuantityShipped INT,
                Principal FLOAT,
                ShippingCharges FLOAT,
                Promotions FLOAT,
                FBAFees FLOAT,
                FixedClosingFee FLOAT,
                VariableClosingFee FLOAT,
                ShippingChargeback FLOAT,
                RefFee FLOAT,
                Total FLOAT
            )
            """)

            # -------------------------------------------------
            # 3. Bulk insert into temp table
            # -------------------------------------------------
            insert_temp = []
            for row in rows:
                row["PostedDate"] = parse_posted_date(row["PostedDate"])
                insert_temp.append((
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

            cur.fast_executemany = True
            cur.executemany("""
                INSERT INTO #TempFinancial VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
            """, insert_temp)

            # -------------------------------------------------
            # 4. Lifecycle delete (SET-BASED, FAST)
            # -------------------------------------------------
            cur.execute("""
            DELETE T
            FROM spapi_app_user.FinancialTransactions T
            JOIN #TempFinancial S
              ON T.AmazonOrderId = S.AmazonOrderId
             AND T.SellerSKU = S.SellerSKU
            WHERE
                (
                    S.TransactionStatus = 'DEFERRED'
                    AND T.TransactionStatus = 'DEFERRED'
                )
             OR (
                    S.TransactionStatus IN ('DEFERRED_RELEASED','RELEASED')
                    AND T.TransactionStatus IN ('DEFERRED','DEFERRED_RELEASED')
                )
            """)

            # -------------------------------------------------
            # 5. Delete exact TransactionIds (idempotency)
            # -------------------------------------------------
            cur.execute("""
            DELETE T
            FROM spapi_app_user.FinancialTransactions T
            JOIN #TempFinancial S
              ON T.TransactionId = S.TransactionId
            """)

            # -------------------------------------------------
            # 6. Insert all new rows
            # -------------------------------------------------
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
            SELECT
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
                DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
                DATEADD(HOUR,4,SYSDATETIMEOFFSET())
            FROM #TempFinancial
            """)

            conn.commit()
            print(f"Upserted {len(rows)} rows in window {window_index}")
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
