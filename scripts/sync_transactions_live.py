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


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# SyncState Helpers
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# LIVE SYNC
# ---------------------------------------------------------

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
        rows = get_transactions(params=params, db_cursor=cur)
    finally:
        cur.close()
        conn.close()

    print(f"get_transactions returned {len(rows) if rows else 0} rows")

    if not rows:
        update_last_sync_at(safe_end)
        return 0

    print("Fast upserting transactions...")

    conn = connect_database()
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # -------------------------------------------------
        # 1. Deduplicate in Python by TransactionId
        # -------------------------------------------------
        unique = {}
        for r in rows:
            tid = r.get("TransactionId")
            if tid:
                unique[tid] = r
        rows = list(unique.values())
        print("After dedup:", len(rows))

        if not rows:
            conn.commit()
            update_last_sync_at(safe_end)
            return 0

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
        # Logic:
        # - If new row is DEFERRED: remove existing DEFERRED for same OrderId+SKU
        # - If new row is DEFERRED_RELEASED: remove DEFERRED + DEFERRED_RELEASED
        # - If new row is RELEASED: remove DEFERRED + DEFERRED_RELEASED (keep RELEASED)
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
        print("Fast upsert complete:", len(rows))

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

    return len(rows)


def main():
    asyncio.run(fetch_and_upsert())


if __name__ == "__main__":
    main()
