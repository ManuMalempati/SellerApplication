#!/usr/bin/env python3

"""
Inventory Ledger Live Sync

Purpose
-------
Continuously ingest Amazon Inventory Ledger adjustment
events into SQL Server.

Important Findings
------------------
- Only EventType='Adjustments' is ingested.
- ReferenceID is unique for adjustment events.
- InventoryLedgerId remains the technical primary key.
- ReferenceID is the adjustment business key.
- Sync is fully idempotent.
- Designed to run every 30 minutes.
- Uses config.SYNC_OVERLAP_HOURS to avoid missing
  late-arriving Amazon adjustment events.

Adjustment Reason Codes
-----------------------
Loss / Damage:
    M
    5
    6
    7
    E
    H
    K
    U

Resolution:
    F
    N

Sync Strategy
-------------
Each sync fetches:

    LastSuccessfulSyncUtc
        minus
    config.SYNC_OVERLAP_HOURS

through

    Current UTC Time

Because ReferenceID is unique for adjustment events,
re-fetching overlapping windows is safe.
"""

import asyncio
import datetime as dt

import config

from app.database import connect_database

from app.inventory_ledger.inventory_ledger import (
    inventory_ledger_detail_report
)

from app.inventory_ledger.database_inventory_ledger import (
    bulk_insert_inventory_ledger
)

from app.utilities.utils import (
    convert_utc_to_utcz_string,
    get_now_iso_string_with_custom_utc_offset
)

SYNC_KEY = "INVENTORY_LEDGER_SYNC"


# ------------------------------------------------------------
# Sync State Helpers
# ------------------------------------------------------------

def get_last_sync(cursor) -> dt.datetime:

    cursor.execute(
        """
        SELECT LastSuccessfulSyncUtc
        FROM spapi_app_user.SyncState
        WHERE SyncKey = ?
        """,
        (SYNC_KEY,)
    )

    row = cursor.fetchone()

    default = dt.datetime(
        2026,
        1,
        1,
        tzinfo=dt.timezone.utc
    )

    if not row:
        return default

    value = row[0]

    if isinstance(value, dt.datetime):

        if value.tzinfo is None:
            value = value.replace(
                tzinfo=dt.timezone.utc
            )

        return value.astimezone(
            dt.timezone.utc
        )

    return default


def update_last_sync_at(ts: dt.datetime):

    ts_utc = ts.astimezone(
        dt.timezone.utc
    )

    ts_naive = ts_utc.replace(
        tzinfo=None
    )

    conn = connect_database()
    cursor = conn.cursor()

    try:

        cursor.execute(
            """
            UPDATE spapi_app_user.SyncState
            SET LastSuccessfulSyncUtc = ?
            WHERE SyncKey = ?
            """,
            (
                ts_naive,
                SYNC_KEY
            )
        )

        if cursor.rowcount == 0:

            cursor.execute(
                """
                INSERT INTO spapi_app_user.SyncState (
                    SyncKey,
                    LastSuccessfulSyncUtc
                )
                VALUES (?, ?)
                """,
                (
                    SYNC_KEY,
                    ts_naive
                )
            )

        conn.commit()

    finally:

        cursor.close()
        conn.close()


# ------------------------------------------------------------
# Main Logic
# ------------------------------------------------------------

async def fetch_and_upsert():

    conn = connect_database()
    cursor = conn.cursor()

    try:
        last_sync = get_last_sync(cursor)

    finally:
        cursor.close()
        conn.close()

    overlap_hours = config.SYNC_OVERLAP_HOURS

    effective_from = (
        last_sync
        - dt.timedelta(hours=overlap_hours)
    )

    end_dt = dt.datetime.now(
        dt.timezone.utc
    )

    start_time = convert_utc_to_utcz_string(
        effective_from
    )

    end_time = convert_utc_to_utcz_string(
        end_dt
    )

    print("------------------------------------------------------------")

    log_ts = get_now_iso_string_with_custom_utc_offset()

    print(
        f"[{log_ts}] Starting Inventory Ledger Sync"
    )

    print(
        f"Last Sync (UTC):       {last_sync.isoformat()}"
    )

    print(
        f"Overlap Hours:         {overlap_hours}"
    )

    print(
        f"Range Start (UTC Z):   {start_time}"
    )

    print(
        f"Range End (UTC Z):     {end_time}"
    )

    print("------------------------------------------------------------")

    rows = await inventory_ledger_detail_report(
        start_time=start_time,
        end_time=end_time
    )

    print(
        f"Fetched {len(rows):,} adjustment rows"
    )

    if not rows:

        update_last_sync_at(end_dt)

        log_ts = get_now_iso_string_with_custom_utc_offset()

        print(
            f"[{log_ts}] No new adjustment rows."
        )

        return 0

    conn = connect_database()
    conn.autocommit = False

    cursor = conn.cursor()

    try:

        inserted = bulk_insert_inventory_ledger(
            cursor=cursor,
            rows=rows
        )

        conn.commit()

    except Exception:

        conn.rollback()

        raise

    finally:

        cursor.close()
        conn.close()

    update_last_sync_at(end_dt)

    log_ts = get_now_iso_string_with_custom_utc_offset()

    print(
        f"[{log_ts}] Inventory Ledger Sync Complete"
    )

    print(
        f"Inserted: {inserted:,}"
    )

    print("------------------------------------------------------------")

    return inserted


def main():
    asyncio.run(
        fetch_and_upsert()
    )


if __name__ == "__main__":
    main()