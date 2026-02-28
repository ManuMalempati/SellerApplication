#!/usr/bin/env python3
from __future__ import annotations
import re
import time
import sys
import os
from typing import Any, Iterable, List, Tuple

# Use centralized config
import config
# Ensure repo root is on sys.path
REPO_ROOT = config.REPO_ROOT
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)
config.load_env()

# Fixed Import: Using absolute import because REPO_ROOT is in sys.path
from app.utils import get_now_iso_string_with_custom_utc_offset

# fail-fast required envs
REQUIRED_ENVS = ["SQLSERVER_CONNECTION_STRING"]
missing_envs = [k for k in REQUIRED_ENVS if not os.getenv(k)]
if missing_envs:
    raise RuntimeError("Missing required env vars: " + ", ".join(missing_envs))

# Config sourced from config.py
SRC_SCHEMA_TABLE = config.INVENTORY_REPORT_TABLE 
STAGING_TABLE = config.INVENTORY_STAGING_TABLE
TARGET_TABLE = config.INVENTORY_TARGET_TABLE
BATCH_SIZE = config.INVENTORY_SYNC_BATCH_SIZE

LOG_DIR = config.LOG_DIR
os.makedirs(LOG_DIR, exist_ok=True)
LOG_PATH = os.path.join(LOG_DIR, "inventorysync.log")

LOCKFILE = config.LOCKFILE
BACKFILL_LOCKFILE = config.BACKFILL_LOCKFILE
LOCK_TIMEOUT_SECONDS = config.LOCK_TIMEOUT_SECONDS
WAIT_FOR_BACKFILL_SECONDS = config.WAIT_FOR_BACKFILL_SECONDS

_cost_re = re.compile(r"[^\d\.\-]")  # strip everything except digits, dot, minus

# logging setup
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

# import DB connector
try:
    from app.database import connect_database
except ImportError:
    raise RuntimeError("Could not import connect_database from app.database. Ensure script is run from repo root.")

# SQL snippets
CREATE_STAGING_SQL = f"""
IF OBJECT_ID(N'{STAGING_TABLE}', 'U') IS NULL
BEGIN
    CREATE TABLE {STAGING_TABLE} (
        PartNumber     NVARCHAR(120) NOT NULL,
        Cost           DECIMAL(18,4) NULL,
        Brand          NVARCHAR(120) NULL,
        Category       NVARCHAR(120) NULL,
        ItemName       NVARCHAR(400) NULL,
        Quantity       INT NULL,
        TotalStock     INT NULL,
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
        ItemName = COALESCE(src.ItemName, target.ItemName),
        Quantity = src.Quantity,
        TotalStock = src.TotalStock,
        IsFulfillable = src.IsFulfillable,
        Source = src.Source,
        LastSeenAt = src.SnapshotAt
WHEN NOT MATCHED BY TARGET THEN
    INSERT (PartNumber, Cost, Brand, Category, ItemName, Quantity, TotalStock, IsFulfillable, Source, LastSeenAt)
    VALUES (src.PartNumber, src.Cost, src.Brand, src.Category, src.ItemName, src.Quantity, src.TotalStock, src.IsFulfillable, src.Source, src.SnapshotAt)
;
"""

SELECT_SQL = f"""
SELECT
    LTRIM(RTRIM(PartNumber)) AS PartNumber,
    Cost AS Cost,
    Brand AS Brand,
    Category AS Category,
    TotalStock AS Quantity,
    ItemName AS ItemName,
    TotalStock AS TotalStock
FROM {SRC_SCHEMA_TABLE}
"""

# helpers
def normalize_partnumber(val: Any) -> str:
    if val is None: return None
    s = str(val).strip()
    return s if s != "" else None

def normalize_cost(val: Any):
    if val is None: return None
    if isinstance(val, (int, float)): return float(val)
    s = str(val).strip()
    if not s or s.lower() in ("not available", "na", "n/a"): return None
    cleaned = _cost_re.sub("", s)
    if cleaned == "" or cleaned == ".": return None
    try: return float(cleaned)
    except: return None

def normalize_text(val: Any) -> Any:
    if val is None: return None
    s = str(val).strip()
    if s == "" or s.lower() in ("not available", "na", "n/a"): return None
    return s

def normalize_quantity(val: Any):
    if val is None: return None
    try: return int(val)
    except:
        s = str(val).strip()
        try: return int(float(s)) if s != "" else None
        except: return None

# DB streaming / insertion
def stream_inventory_rows(read_cursor, select_sql: str, batch_size: int = 1000) -> Iterable[Tuple]:
    try:
        read_cursor.execute("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED")
    except Exception:
        logger.debug("Failed to set transaction isolation level (continuing): %s", sys.exc_info()[1])
    read_cursor.execute(select_sql)
    while True:
        rows = read_cursor.fetchmany(batch_size)
        if not rows: break
        for r in rows: yield r

def build_insert_tuples(rows: Iterable[Tuple]) -> List[Tuple]:
    # Use your custom logger timestamp for the snapshot record
    now = get_now_iso_string_with_custom_utc_offset()
    out = []
    for r in rows:
        partnum = normalize_partnumber(r[0])
        if partnum is None: continue
        
        cost = normalize_cost(r[1])
        brand = normalize_text(r[2])
        category = normalize_text(r[3])
        qty = normalize_quantity(r[4])
        item_name = normalize_text(r[5]) if len(r) > 5 else None
        total_stock = normalize_quantity(r[6]) if len(r) > 6 else None
        is_ful = None
        source = "InventoryReport"
        
        # SQL Server DATETIME2 accepts ISO strings automatically
        snapshot_at = now
        
        out.append((partnum, cost, brand, category, item_name, qty, total_stock, is_ful, source, snapshot_at))
    return out

def insert_into_staging(write_cursor, tuples: List[Tuple]):
    insert_sql = f"""
    INSERT INTO {STAGING_TABLE} (PartNumber, Cost, Brand, Category, ItemName, Quantity, TotalStock, IsFulfillable, Source, SnapshotAt)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    try:
        write_cursor.fast_executemany = True
    except: pass
    write_cursor.executemany(insert_sql, tuples)

# Lockfile helpers
def _acquire_lockfile(path: str, timeout_seconds: int = None) -> bool:
    try:
        flags = os.O_CREAT | os.O_EXCL | os.O_RDWR
        fd = os.open(path, flags)
        with os.fdopen(fd, "w") as fh:
            fh.write(f"{os.getpid()}\n{time.time()}\n")
        logger.info("Acquired lockfile: %s", path)
        return True
    except FileExistsError:
        if timeout_seconds:
            try:
                st = os.stat(path)
                age = time.time() - st.st_mtime
                if age > timeout_seconds:
                    logger.warning("Lockfile %s stale (age %.0f s). Removing.", path, age)
                    try: os.remove(path)
                    except: return False
                    return _acquire_lockfile(path, timeout_seconds)
            except: pass
        return False
    except: return False

def acquire_lock() -> bool:
    return _acquire_lockfile(LOCKFILE, LOCK_TIMEOUT_SECONDS)

def release_lock():
    try:
        if os.path.exists(LOCKFILE):
            os.remove(LOCKFILE)
            logger.info("Released lock: %s", LOCKFILE)
    except: pass

def wait_for_backfill_clear(max_wait: int) -> bool:
    if not os.path.exists(BACKFILL_LOCKFILE):
        return True
    logger.info("Backfill lock present. Waiting up to %d seconds...", max_wait)
    start = time.time()
    while time.time() - start < max_wait:
        if not os.path.exists(BACKFILL_LOCKFILE): return True
        time.sleep(5)
    return False

# Main sync flow
def run_sync():
    if not wait_for_backfill_clear(WAIT_FOR_BACKFILL_SECONDS):
        logger.warning("Backfill lock still present; skipping run.")
        return 0

    read_conn = connect_database()
    write_conn = connect_database()
    if read_conn is None or write_conn is None:
        raise RuntimeError("Database connection(s) failed")
    
    write_conn.autocommit = False
    read_cursor = read_conn.cursor()
    write_cursor = write_conn.cursor()

    try:
        logger.info("Ensuring staging table exists...")
        write_cursor.execute(CREATE_STAGING_SQL)
        write_conn.commit()

        logger.info("Truncating staging...")
        write_cursor.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        inserted_rows = 0
        stream = stream_inventory_rows(read_cursor, SELECT_SQL, batch_size=BATCH_SIZE)
        buffer = []
        for src_row in stream:
            buffer.append(src_row)
            if len(buffer) >= BATCH_SIZE:
                tuples = build_insert_tuples(buffer)
                if tuples:
                    insert_into_staging(write_cursor, tuples)
                    inserted_rows += len(tuples)
                buffer = []
        if buffer:
            tuples = build_insert_tuples(buffer)
            if tuples:
                insert_into_staging(write_cursor, tuples)
                inserted_rows += len(tuples)

        write_conn.commit()
        logger.info("Staging complete. Inserted %d rows.", inserted_rows)

        logger.info("Running MERGE into Target...")
        start = time.time()
        write_cursor.execute(MERGE_SQL)
        write_conn.commit()
        logger.info("MERGE completed in %.2fs.", time.time() - start)

        write_cursor.execute(TRUNCATE_STAGING_SQL)
        write_conn.commit()

        return inserted_rows
    except Exception as e:
        write_conn.rollback()
        logger.error("Error during sync: %s", e)
        raise
    finally:
        read_conn.close()
        write_conn.close()

def main():
    current_ts = get_now_iso_string_with_custom_utc_offset()
    logger.info("Starting sync: %s", current_ts)
    
    if not acquire_lock():
        logger.info("Another instance is running; exiting.")
        return 0
    try:
        return run_sync()
    finally:
        release_lock()
        logger.info("Done: %s", get_now_iso_string_with_custom_utc_offset())

if __name__ == "__main__":
    try:
        main()
        sys.exit(0)
    except Exception:
        sys.exit(1)