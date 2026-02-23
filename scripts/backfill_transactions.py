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


async def run_backfill(start_date: datetime, end_date: datetime):
    print("Backfill starting")
    print(f"Start: {fmt(start_date)}")
    print(f"End:   {fmt(end_date)}")

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

        print(f"\nWindow {window_index}: {params['postedAfter']} -> {params['postedBefore']}")

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

            tids = [row["TransactionId"] for row in rows]
            placeholders = ",".join("?" for _ in tids)

            cur.execute(
                f"DELETE FROM spapi_app_user.FinancialTransactions WHERE TransactionId IN ({placeholders})",
                tids
            )

            insert_values = []
            for row in rows:
                row["PostedDate"] = parse_posted_date(row["PostedDate"])

                insert_values.append((
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
                    safe_decimal(row["Commission"]),
                    safe_decimal(row["FixedClosingFee"]),
                    safe_decimal(row["VariableClosingFee"]),
                    safe_decimal(row["ShippingChargeback"]),
                    safe_decimal(row["RefFee"]),
                    safe_decimal(row["Total"]),
                ))

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
                    ?,?,?,?,?,?,?,?,
                    DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
                    DATEADD(HOUR,4,SYSDATETIMEOFFSET())
                )
            """, insert_values)

            conn.commit()
            total_upserted += len(rows)
            print(f"Upserted {len(rows)} rows")

        except Exception as exc:
            conn.rollback()
            print("Database error:", exc)
        finally:
            cur.close()
            conn.close()

        window_start = window_end

    print(f"\nBackfill complete. Total rows upserted: {total_upserted}")