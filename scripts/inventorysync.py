# RESPONSIBLE FOR InventoryReportCopy Table

from __future__ import annotations
import os, sys, time, re

import config
from app.database import connect_database
from app.utilities.utils import now_utc_plus_offset_naive

# Config
SRC_SCHEMA_TABLE = config.INVENTORY_REPORT_TABLE
STAGING_TABLE = config.INVENTORY_STAGING_TABLE
TARGET_TABLE = config.INVENTORY_TARGET_TABLE
BATCH_SIZE = config.INVENTORY_SYNC_BATCH_SIZE
LOCKFILE = config.LOCKFILE
BACKFILL_LOCKFILE = config.BACKFILL_LOCKFILE
LOCK_TIMEOUT_SECONDS = config.LOCK_TIMEOUT_SECONDS
WAIT_FOR_BACKFILL_SECONDS = config.WAIT_FOR_BACKFILL_SECONDS

_cost_re = re.compile(r"[^\d\.\-]")

# SQL
CREATE_STAGING_SQL = f"""
IF OBJECT_ID(N'{STAGING_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {STAGING_TABLE} (
        PartNumber NVARCHAR(120) NOT NULL,
        Cost DECIMAL(18,4) NULL,
        Brand NVARCHAR(120) NULL,
        Category NVARCHAR(120) NULL,
        ItemName NVARCHAR(400) NULL,
        Quantity INT NULL,
        TotalStock INT NULL,
        IsFulfillable BIT NULL,
        Source NVARCHAR(64) NULL,
        SnapshotAt DATETIME2(0) NOT NULL
    );
END
"""

TRUNCATE_STAGING_SQL = f"TRUNCATE TABLE {STAGING_TABLE};"

MERGE_SQL = f"""
MERGE INTO {TARGET_TABLE} AS target
USING {STAGING_TABLE} AS src
ON target.PartNumber = src.PartNumber
WHEN MATCHED THEN UPDATE SET
    Cost = src.Cost,
    Brand = src.Brand,
    Category = src.Category,
    ItemName = COALESCE(src.ItemName, target.ItemName),
    Quantity = src.Quantity,
    TotalStock = src.TotalStock,
    IsFulfillable = src.IsFulfillable,
    Source = src.Source,
    LastSeenAt = src.SnapshotAt
WHEN NOT MATCHED BY TARGET THEN
INSERT (PartNumber, Cost, Brand, Category, ItemName, Quantity, TotalStock, IsFulfillable, Source, LastSeenAt)
VALUES (src.PartNumber, src.Cost, src.Brand, src.Category, src.ItemName, src.Quantity, src.TotalStock, src.IsFulfillable, src.Source, src.SnapshotAt);
"""

SELECT_SQL = f"""
SELECT
    LTRIM(RTRIM(PartNumber)) AS PartNumber,
    Cost,
    Brand,
    Category,
    TotalStock AS Quantity,
    ItemName,
    TotalStock
FROM {SRC_SCHEMA_TABLE}
"""

# Normalizers
def normalize_partnumber(v):
    if v is None: return None
    s = str(v).strip()
    return s or None

def normalize_cost(v):
    if v is None: return None
    if isinstance(v, (int, float)): return float(v)
    s = str(v).strip()
    if not s or s.lower() in ("na", "n/a", "not available"): return None
    cleaned = _cost_re.sub("", s)
    if cleaned in ("", "."): return None
    try: return float(cleaned)
    except: return None

def normalize_text(v):
    if v is None: return None
    s = str(v).strip()
    if not s or s.lower() in ("na", "n/a", "not available"): return None
    return s

def normalize_quantity(v):
    if v is None: return None
    try: return int(v)
    except:
        try: return int(float(str(v).strip()))
        except: return None

# Streaming
def stream_inventory_rows(cur, sql, batch_size=1000):
    try:
        cur.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    except:
        pass
    cur.execute(sql)
    while True:
        rows = cur.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            yield r

def build_insert_tuples(rows):
    snapshot_at = now_utc_plus_offset_naive()
    out = []
    for r in rows:
        pn = normalize_partnumber(r[0])
        if pn is None:
            continue
        out.append((
            pn,
            normalize_cost(r[1]),
            normalize_text(r[2]),
            normalize_text(r[3]),
            normalize_text(r[5]) if len(r) > 5 else None,
            normalize_quantity(r[4]),
            normalize_quantity(r[6]) if len(r) > 6 else None,
            None,
            "InventoryReport",
            snapshot_at
        ))
    return out

def insert_into_staging(cur, tuples):
    sql = f"""
    INSERT INTO {STAGING_TABLE}
    (PartNumber, Cost, Brand, Category, ItemName, Quantity, TotalStock, IsFulfillable, Source, SnapshotAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        cur.fast_executemany = True
    except:
        pass
    cur.executemany(sql, tuples)

# Locking
def _acquire_lockfile(path, timeout=None):
    try:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        print(f"Acquired lockfile: {path}")
        return True
    except FileExistsError:
        if timeout:
            try:
                age = time.time() - os.stat(path).st_mtime
                if age > timeout:
                    print(f"Stale lockfile {path} (age {age:.0f}s). Removing.")
                    os.remove(path)
                    return _acquire_lockfile(path, timeout)
            except:
                pass
        return False
    except:
        return False

def acquire_lock():
    return _acquire_lockfile(LOCKFILE, LOCK_TIMEOUT_SECONDS)

def release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
            print(f"Released lock: {LOCKFILE}")
    except:
        pass

def wait_for_backfill_clear(max_wait):
    if not os.path.exists(BACKFILL_LOCKFILE):
        return True
    print(f"Backfill lock present. Waiting up to {max_wait} seconds...")
    start = time.time()
    while time.time() - start < max_wait:
        if not os.path.exists(BACKFILL_LOCKFILE):
            print("Backfill lock cleared.")
            return True
        time.sleep(5)
    print("Backfill lock still present; skipping run.")
    return False

# Main sync
def run_sync():
    if not wait_for_backfill_clear(WAIT_FOR_BACKFILL_SECONDS):
        return 0

    read_conn = connect_database()
    write_conn = connect_database()
    write_conn.autocommit = False

    rc = read_conn.cursor()
    wc = write_conn.cursor()

    try:
        print("Ensuring staging table exists...")
        wc.execute(CREATE_STAGING_SQL)
        write_conn.commit()

        print("Truncating staging table...")
        wc.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        inserted = 0
        buffer = []

        print("Streaming rows from source...")
        for row in stream_inventory_rows(rc, SELECT_SQL, BATCH_SIZE):
            buffer.append(row)
            if len(buffer) >= BATCH_SIZE:
                tuples = build_insert_tuples(buffer)
                insert_into_staging(wc, tuples)
                inserted += len(tuples)
                buffer = []

        if buffer:
            tuples = build_insert_tuples(buffer)
            insert_into_staging(wc, tuples)
            inserted += len(tuples)

        write_conn.commit()
        print(f"Inserted {inserted} rows into staging.")

        print("Running MERGE...")
        wc.execute(MERGE_SQL)
        write_conn.commit()

        print("Cleaning staging table...")
        wc.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        print("Inventory sync finished.")
        return inserted

    except Exception as e:
        try:
            write_conn.rollback()
        except:
            pass
        print("Error during inventory sync:", e)
        raise

    finally:
        rc.close()
        wc.close()
        read_conn.close()
        write_conn.close()

def main():
    print("Starting inventory sync:", now_utc_plus_offset_naive())
    if not acquire_lock():
        print("Another instance is running; exiting.")
        return 0
    try:
        return run_sync()
    finally:
        release_lock()
        print("Done:", now_utc_plus_offset_naive())

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception:
        print("Fatal error in inventorysync")
        sys.exit(1)