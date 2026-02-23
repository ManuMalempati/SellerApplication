#!/usr/bin/env python3
import sys
import os
import asyncio
import datetime as dt

from . import config
config.load_env()

from app.transactions import get_transactions
from app.database import connect_database


SYNC_KEY = "TRANSACTIONS_LIVE_SYNC"


def parse_posted_date(value):
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except:
        return None


def safe_decimal(value):
    try:
        return float(value)
    except:
        return None


def get_last_sync(cursor) -> dt.datetime:
    cursor.execute(
        "SELECT LastSuccessfulSyncUtc FROM spapi_app_user.SyncState WHERE SyncKey = ?",
        (SYNC_KEY,)
    )
    row = cursor.fetchone()
    default = dt.datetime(2025, 1, 1, tzinfo=dt.timezone.utc)

    if not row or not row[0]:
        return default

    val = row[0]
    if isinstance(val, str):
        try:
            parsed = dt.datetime.fromisoformat(val.replace("Z", "+00:00"))
            return parsed.astimezone(dt.timezone.utc)
        except:
            return default

    if isinstance(val, dt.datetime):
        return val.astimezone(dt.timezone.utc)

    return default


def update_last_sync_at(ts: dt.datetime):
    ts_utc = ts.astimezone(dt.timezone.utc)
    ts_naive = ts_utc.replace(tzinfo=None)

    conn = connect_database()
    cur = conn.cursor()
    try:
        cur.execute(
            "UPDATE spapi_app_user.SyncState SET LastSuccessfulSyncUtc = ? WHERE SyncKey = ?",
            (ts_naive, SYNC_KEY)
        )
        if cur.rowcount == 0:
            cur.execute(
                "INSERT INTO spapi_app_user.SyncState (SyncKey, LastSuccessfulSyncUtc) VALUES (?, ?)",
                (SYNC_KEY, ts_naive)
            )
        conn.commit()
    finally:
        cur.close()
        conn.close()


async def fetch_and_upsert():
    # Load last sync
    conn = connect_database()
    cur = conn.cursor()
    try:
        last_sync = get_last_sync(cur)
    finally:
        cur.close()
        conn.close()

    overlap_hours = config.SYNC_OVERLAP_HOURS
    effective_from = last_sync - dt.timedelta(hours=overlap_hours)
    posted_after = effective_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    now_utc = dt.datetime.now(dt.timezone.utc)
    safe_end = now_utc - dt.timedelta(minutes=2)
    posted_before = safe_end.strftime("%Y-%m-%dT%H:%M:%SZ")

    params = {
        "postedAfter": posted_after,
        "postedBefore": posted_before,
    }

    print("------------------------------------------------------------")
    print("Starting LIVE transaction sync")
    print(f"LastSuccessfulSyncUtc: {last_sync.isoformat()}")
    print(f"EffectiveFrom (UTC):   {posted_after}")
    print(f"EffectiveTo (UTC):     {posted_before}")
    print("------------------------------------------------------------")

    # Fetch transactions
    conn = connect_database()
    cur = conn.cursor()
    try:
        items = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    print(f"get_transactions returned {len(items) if items else 0} rows")

    if not items:
        update_last_sync_at(safe_end)
        return 0

    print("Upserting transactions...")

    conn = connect_database()
    conn.autocommit = False
    cur = conn.cursor()

    try:
        # 1. Delete all rows for these TransactionIds
        tids = [row["TransactionId"] for row in items]
        placeholders = ",".join("?" for _ in tids)

        cur.execute(
            f"DELETE FROM spapi_app_user.FinancialTransactions WHERE TransactionId IN ({placeholders})",
            tids
        )

        # 2. Prepare batch insert values
        insert_values = []
        for row in items:
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

    except Exception as exc:
        conn.rollback()
        print("ERROR during UPSERT:", exc)
        raise
    finally:
        cur.close()
        conn.close()

    update_last_sync_at(safe_end)
    print("Transaction sync completed successfully.")
    print("------------------------------------------------------------")

    return len(items)


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()