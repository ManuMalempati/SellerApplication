#!/usr/bin/env python3
"""
inventorysync.py

Non-invasive inventory sync (safe for production InventoryReport).

- Uses two separate DB connections:
    * read_conn (dedicated) to SELECT from InventoryReport and stream rows
    * write_conn (dedicated) to CREATE/TRUNCATE staging, INSERT batches and MERGE -> CurrentInventory
- Assumes InventoryReport has: PartNumber, Cost, Brand, Category, TotalStock
- Intended to live in gcinventory/scripts and be run by a scheduler.
- Adds a lockfile and rotating logs so you can run it directly (no wrapper needed).
"""

import re
import time
import sys
import os
from datetime import datetime, timezone
from typing import Any, Iterable, List, Tuple

# ensure project root is importable (gcinventory/)
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

from dotenv import load_dotenv

ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# import connect_database from your project
try:
    from app.database import connect_database  # type: ignore
except Exception:
    try:
        from database import connect_database  # type: ignore
    except Exception:
        raise RuntimeError("Could not import connect_database from app.database or database.py. Run this script from gcinventory/scripts or adjust imports.")

# Config
SRC_SCHEMA_TABLE = os.getenv("INVENTORY_REPORT_TABLE", "dbo.InventoryReport")  # e.g. dbo.InventoryReport
STAGING_TABLE = os.getenv("INVENTORY_STAGING_TABLE", "spapi_app_user.InventoryStaging")
TARGET_TABLE = os.getenv("INVENTORY_TARGET_TABLE", "spapi_app_user.CurrentInventory")
BATCH_SIZE = int(os.getenv("INVENTORY_SYNC_BATCH_SIZE", "1000"))

# Lock + logging configuration
LOG_DIR = os.path.join(REPO_ROOT, "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "inventorysync.log")
LOCKFILE = os.path.join(REPO_ROOT, "inventorysync.lock")
LOCK_TIMEOUT_SECONDS = int(os.getenv("INVENTORY_SYNC_LOCK_TIMEOUT_SECONDS", str(6 * 3600)))  # default 6 hours

_cost_re = re.compile(r"[^\d\.\-]")  # strip everything except digits, dot, minus

# -------------------------
# Logging setup (rotating)
# -------------------------
import logging
from logging.handlers import RotatingFileHandler

logger = logging.getLogger("inventorysync")
logger.setLevel(logging.INFO)

file_handler = RotatingFileHandler(LOG_PATH, maxBytes=5 * 1024 * 1024, backupCount=5, encoding="utf-8")
file_formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

console_handler = logging.StreamHandler()
console_handler.setFormatter(file_formatter)
logger.addHandler(console_handler)


# -------------------------
# Normalizers / helpers
# -------------------------
def normalize_partnumber(val: Any) -> str:
    if val is None:
        return None
    s = str(val).strip()
    return s if s != "" else None


def normalize_cost(val: Any):
    if val is None:
        return None
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip()
    if not s or s.lower() in ("not available", "na", "n/a"):
        return None
    cleaned = _cost_re.sub("", s)
    if cleaned == "" or cleaned == ".":
        return None
    try:
        return float(cleaned)
    except Exception:
        return None


def normalize_text(val: Any) -> Any:
    if val is None:
        return None
    s = str(val).strip()
    if s == "":
        return None
    if s.lower() in ("not available", "na", "n/a"):
        return None
    return s


def normalize_quantity(val: Any):
    if val is None:
        return None
    try:
        return int(val)
    except Exception:
        s = str(val).strip()
        if s == "":
            return None
        try:
            return int(float(s))
        except Exception:
            return None


# -------------------------
# SQL snippets
# -------------------------
CREATE_STAGING_SQL = f"""
IF OBJECT_ID(N'{STAGING_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {STAGING_TABLE} (
        PartNumber     NVARCHAR(120) NOT NULL,
        Cost           DECIMAL(18,4) NULL,
        Brand          NVARCHAR(120) NULL,
        Category       NVARCHAR(120) NULL,
        Quantity       INT NULL,
        IsFulfillable  BIT NULL,
        Source         NVARCHAR(64) NULL,
        SnapshotAt     DATETIME2(0) NOT NULL
    );
END
"""

TRUNCATE_STAGING_SQL = f"TRUNCATE TABLE {STAGING_TABLE};"

MERGE_SQL = f"""
MERGE INTO {TARGET_TABLE} AS target
USING {STAGING_TABLE} AS src
    ON target.PartNumber = src.PartNumber
WHEN MATCHED THEN
    UPDATE SET
        Cost = src.Cost,
        Brand = src.Brand,
        Category = src.Category,
        Quantity = src.Quantity,
        IsFulfillable = src.IsFulfillable,
        Source = src.Source,
        LastSeenAt = src.SnapshotAt
WHEN NOT MATCHED BY TARGET THEN
    INSERT (PartNumber, Cost, Brand, Category, Quantity, IsFulfillable, Source, LastSeenAt)
    VALUES (src.PartNumber, src.Cost, src.Brand, src.Category, src.Quantity, src.IsFulfillable, src.Source, src.SnapshotAt)
;
"""

SELECT_SQL = f"""
SELECT
    LTRIM(RTRIM(PartNumber)) AS PartNumber,
    Cost AS Cost,
    Brand AS Brand,
    Category AS Category,
    TotalStock AS Quantity
FROM {SRC_SCHEMA_TABLE}
"""


# -------------------------
# DB streaming / insertion
# -------------------------
def stream_inventory_rows(read_cursor, select_sql: str, batch_size: int = 1000) -> Iterable[Tuple]:
    """
    Execute the SELECT against the source (read-only) and yield rows in batches.
    Uses a dedicated read_cursor so it doesn't interfere with write operations.
    """
    try:
        read_cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    except Exception:
        logger.debug("Failed to set transaction isolation level (continuing): %s", sys.exc_info()[1])
    read_cursor.execute(select_sql)
    while True:
        rows = read_cursor.fetchmany(batch_size)
        if not rows:
            break
        for r in rows:
            yield r


def build_insert_tuples(rows: Iterable[Tuple]) -> List[Tuple]:
    # use naive UTC for DB (SQL drivers typically expect naive datetimes)
    now = datetime.now(timezone.utc).replace(microsecond=0).replace(tzinfo=None)
    out = []
    for r in rows:
        partnum = normalize_partnumber(r[0])
        if partnum is None:
            continue
        cost = normalize_cost(r[1])
        brand = normalize_text(r[2])
        category = normalize_text(r[3])
        qty = normalize_quantity(r[4])
        is_ful = None
        source = "InventoryReport"
        snapshot_at = now
        out.append((partnum, cost, brand, category, qty, is_ful, source, snapshot_at))
    return out


def insert_into_staging(write_cursor, tuples: List[Tuple]):
    insert_sql = f"""
    INSERT INTO {STAGING_TABLE} (PartNumber, Cost, Brand, Category, Quantity, IsFulfillable, Source, SnapshotAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        write_cursor.fast_executemany = True
    except Exception:
        pass
    write_cursor.executemany(insert_sql, tuples)


# -------------------------
# Lockfile helpers
# -------------------------
def acquire_lock() -> bool:
    """Create lockfile atomically. If present and not stale, return False."""
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        fd = os.open(LOCKFILE, flags)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        logger.info("Acquired lock: %s", LOCKFILE)
        return True
    except FileExistsError:
        try:
            st = os.stat(LOCKFILE)
            age = time.time() - st.st_mtime
            if age > LOCK_TIMEOUT_SECONDS:
                logger.warning("Lockfile is stale (age %.0f s). Removing and acquiring.", age)
                try:
                    os.remove(LOCKFILE)
                except Exception:
                    logger.exception("Failed to remove stale lockfile")
                    return False
                return acquire_lock()
            else:
                logger.info("Lockfile exists and is recent (age %.0f s). Exiting to avoid overlap.", age)
                return False
        except Exception:
            logger.exception("Error inspecting lockfile; refusing to run")
            return False


def release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
            logger.info("Released lock: %s", LOCKFILE)
    except Exception:
        logger.exception("Failed to remove lockfile on exit")


# -------------------------
# Main sync flow
# -------------------------
def run_sync():
    # open two separate connections so read resultset doesn't block writes
    read_conn = connect_database()
    write_conn = connect_database()
    if read_conn is None or write_conn is None:
        raise RuntimeError("Database connection(s) failed")
    # control transactions separately on write_conn; read_conn left default
    write_conn.autocommit = False

    read_cursor = read_conn.cursor()
    write_cursor = write_conn.cursor()

    try:
        # ensure staging exists (DDL only on staging/target; NO changes to source)
        logger.info("Ensuring staging table exists (if missing)...")
        write_cursor.execute(CREATE_STAGING_SQL)
        write_conn.commit()

        logger.info("Truncating staging table...")
        write_cursor.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        logger.info("Streaming rows from source (read-only) using a dedicated connection...")
        inserted_rows = 0
        batch_inserts = 0

        stream = stream_inventory_rows(read_cursor, SELECT_SQL, batch_size=BATCH_SIZE)
        buffer: List[Tuple] = []
        for src_row in stream:
            buffer.append(src_row)
            if len(buffer) >= BATCH_SIZE:
                tuples = build_insert_tuples(buffer)
                if tuples:
                    logger.info("Inserting batch of %d rows into staging...", len(tuples))
                    insert_into_staging(write_cursor, tuples)
                    batch_inserts += 1
                    inserted_rows += len(tuples)
                buffer = []
        if buffer:
            tuples = build_insert_tuples(buffer)
            if tuples:
                logger.info("Inserting final batch of %d rows into staging...", len(tuples))
                insert_into_staging(write_cursor, tuples)
                batch_inserts += 1
                inserted_rows += len(tuples)

        write_conn.commit()
        logger.info("Inserted total %d rows into staging across %d batches.", inserted_rows, batch_inserts)

        # MERGE into target using write_conn
        logger.info("Running MERGE into CurrentInventory...")
        start = time.time()
        write_cursor.execute(MERGE_SQL)
        try:
            affected = write_cursor.rowcount
        except Exception:
            affected = None
        write_conn.commit()
        elapsed = time.time() - start
        logger.info("MERGE completed in %.2fs. @@ROWCOUNT (approx): %s", elapsed, affected)

        # cleanup staging
        logger.info("Truncating staging table (cleanup)...")
        write_cursor.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        logger.info("Inventory sync finished successfully.")
    except Exception as e:
        # rollback writes only
        try:
            write_conn.rollback()
        except Exception:
            logger.exception("Failed to rollback write connection")
        logger.exception("Error during inventory sync: %s", e)
        raise
    finally:
        try:
            read_cursor.close()
            read_conn.close()
        except Exception:
            pass
        try:
            write_cursor.close()
            write_conn.close()
        except Exception:
            pass


def main():
    logger.info("Starting inventory sync: %s", datetime.now(timezone.utc).isoformat() + "Z")
    if not acquire_lock():
        logger.info("Another instance is running or lock could not be acquired; exiting.")
        return 0
    try:
        run_sync()
        return 0
    finally:
        release_lock()
        logger.info("Done: %s", datetime.now(timezone.utc).isoformat() + "Z")


if __name__ == "__main__":
    try:
        exit_code = main() or 0
        sys.exit(exit_code)
    except Exception:
        logger.exception("Fatal error in inventorysync")
        raise