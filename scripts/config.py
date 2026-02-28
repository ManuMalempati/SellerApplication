import os
from pathlib import Path
from typing import Optional
from datetime import datetime, timezone, timedelta

try:
    from dotenv import load_dotenv as _load_dotenv
except Exception:
    def _load_dotenv(path=None):
        return False

SCRIPTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, ".."))
ENV_PATH = os.path.join(REPO_ROOT, ".env")

def load_env():
    # load .env if present (idempotent)
    if os.path.exists(ENV_PATH):
        _load_dotenv(ENV_PATH)

def get_env(key: str, default: Optional[str] = None) -> Optional[str]:
    return os.getenv(key, default)

def get_int(key: str, default: int) -> int:
    val = os.getenv(key)
    try:
        return int(val) if val is not None else default
    except Exception:
        return default

def get_float(key: str, default: float) -> float:
    val = os.getenv(key)
    try:
        return float(val) if val is not None else default
    except Exception:
        return default
    
_divisor_raw = os.getenv("GOVT_VAT_RATE_DIVISOR")
if _divisor_raw:
    try:
        _divisor_val = float(_divisor_raw)
        GOVT_VAT_RATE = 1 / _divisor_val if _divisor_val != 0 else 0.0
    except ValueError:
        GOVT_VAT_RATE = 0.0
else:
    GOVT_VAT_RATE = 0.0

# Generic defaults and env-driven config used by scripts
# Sync orders
SYNC_OVERLAP_HOURS = get_int("SYNC_OVERLAP_HOURS", 2)

# In what time intervals should backfill orders process 
BACKFILL_CHUNK_DAYS = get_int("BACKFILL_CHUNK_DAYS", 1)

# Inventory sync defaults
INVENTORY_REPORT_TABLE = get_env("INVENTORY_REPORT_TABLE") or "dbo.InventoryReport"
INVENTORY_STAGING_TABLE = get_env("INVENTORY_STAGING_TABLE") or "spapi_app_user.InventoryStaging"
INVENTORY_TARGET_TABLE = get_env("INVENTORY_TARGET_TABLE") or "spapi_app_user.InventoryReportCopy"
INVENTORY_SYNC_BATCH_SIZE = get_int("INVENTORY_SYNC_BATCH_SIZE", 1000)

LOG_DIR = os.path.join(REPO_ROOT, "logs")
LOCKFILE = os.path.join(REPO_ROOT, "inventorysync.lock")
BACKFILL_LOCKFILE = os.path.join(REPO_ROOT, "backfill.lock")
LOCK_TIMEOUT_SECONDS = get_int("INVENTORY_SYNC_LOCK_TIMEOUT_SECONDS", 6 * 3600)
WAIT_FOR_BACKFILL_SECONDS = get_int("INVENTORY_WAIT_FOR_BACKFILL_SECONDS", 120)
