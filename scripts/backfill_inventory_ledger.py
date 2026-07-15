import asyncio

from datetime import datetime, timezone

from app.database import connect_database
from app.inventory_ledger.inventory_ledger import (
    inventory_ledger_detail_report
)
from app.inventory_ledger.database_inventory_ledger import (
    bulk_insert_inventory_ledger
)


async def backfill_inventory_ledger():

    start_time = "2026-01-01T00:00:00Z"

    end_time = datetime.now(
        timezone.utc
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    print("=" * 60)
    print("INVENTORY LEDGER BACKFILL")
    print("=" * 60)
    print(f"Start: {start_time}")
    print(f"End:   {end_time}")
    print("=" * 60)

    print("Fetching Inventory Ledger...")

    rows = await inventory_ledger_detail_report(
        start_time=start_time,
        end_time=end_time
    )

    print(f"\nFetched {len(rows):,} rows")

    if not rows:
        print("No rows returned. Exiting.")
        return

    conn = connect_database()
    cursor = conn.cursor()

    try:

        print("Inserting rows...")

        inserted = bulk_insert_inventory_ledger(
            cursor=cursor,
            rows=rows
        )

        conn.commit()

        print(
            f"Successfully inserted "
            f"{inserted:,} rows"
        )

    except Exception as e:

        conn.rollback()

        print("Backfill failed.")
        print(e)

        raise

    finally:

        cursor.close()
        conn.close()

    print("=" * 60)
    print("BACKFILL COMPLETE")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(
        backfill_inventory_ledger()
    )