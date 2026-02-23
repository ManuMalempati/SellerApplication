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
        cleaned = val.strip().replace("\u200b", "").replace("\ufeff", "")
        try:
            parsed = dt.datetime.fromisoformat(cleaned.replace("Z", "+00:00"))
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=dt.timezone.utc)
            return parsed.astimezone(dt.timezone.utc)
        except:
            return default

    if isinstance(val, dt.datetime):
        if val.tzinfo is None:
            val = val.replace(tzinfo=dt.timezone.utc)
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

    effective_from = (last_sync - dt.timedelta(hours=overlap_hours)).replace(microsecond=0)
    posted_after = effective_from.strftime("%Y-%m-%dT%H:%M:%SZ")

    # Amazon requires postedBefore <= now - 2 minutes
    now_utc = dt.datetime.now(dt.timezone.utc)
    safe_end = now_utc - dt.timedelta(minutes=2)

    posted_before = safe_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_dt = safe_end  # store this in SyncState

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

    print("Calling get_transactions...")
    conn = connect_database()
    cur = conn.cursor()
    try:
        items = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    print(f"get_transactions returned {len(items) if items else 0} rows")

    if not items:
        update_last_sync_at(end_dt)
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
            insert_values.append((
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

        # 3. Batch insert
        cur.fast_executemany = True
        cur.executemany("""
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
        """, insert_values)

        conn.commit()

    except Exception as exc:
        conn.rollback()
        print("ERROR during UPSERT:", exc)
        raise
    finally:
        cur.close()
        conn.close()

    update_last_sync_at(end_dt)
    print("Transaction sync completed successfully.")
    print("------------------------------------------------------------")

    return len(items)


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()