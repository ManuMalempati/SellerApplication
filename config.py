import os
from dotenv import load_dotenv

# -------------------------------------------------
# Load .env (idempotent)
# -------------------------------------------------
load_dotenv()

# -------------------------------------------------
# Path Setup
# -------------------------------------------------
SCRIPTS_DIR = os.path.dirname(__file__)
REPO_ROOT = os.path.abspath(os.path.join(SCRIPTS_DIR, ".."))

# -------------------------------------------------
# VAT Calculation
# -------------------------------------------------
_divisor_raw = os.getenv("GOVT_VAT_RATE_DIVISOR")
if _divisor_raw:
    try:
        _divisor_val = float(_divisor_raw)
        GOVT_VAT_RATE = 1 / _divisor_val if _divisor_val != 0 else 0.0
    except (ValueError, TypeError):
        GOVT_VAT_RATE = 0.0
else:
    GOVT_VAT_RATE = 0.0

# -------------------------------------------------
# Orders Sync Config
# -------------------------------------------------
SYNC_OVERLAP_HOURS = int(os.getenv("SYNC_OVERLAP_HOURS", "2"))
BACKFILL_CHUNK_DAYS = int(os.getenv("BACKFILL_CHUNK_DAYS", "1"))
BACKFILL_DAYS = int(os.getenv("BACKFILL_DAYS", "56"))
RETURNS_DATA_DAYS = int(os.getenv("RETURNS_DATA_DAYS", "35"))
UTC_OFFSET = int(os.getenv("UTC_OFFSET", "4"))

# -------------------------------------------------
# Inventory Sync Config
# -------------------------------------------------
INVENTORY_REPORT_TABLE = os.getenv("INVENTORY_REPORT_TABLE", "dbo.InventoryReport")
INVENTORY_STAGING_TABLE = os.getenv("INVENTORY_STAGING_TABLE", "spapi_app_user.InventoryStaging")
INVENTORY_TARGET_TABLE = os.getenv("INVENTORY_TARGET_TABLE", "spapi_app_user.InventoryReportCopy")
INVENTORY_SYNC_BATCH_SIZE = int(os.getenv("INVENTORY_SYNC_BATCH_SIZE", "1000"))

# -------------------------------------------------
# Paths & Lockfiles
# -------------------------------------------------
LOG_DIR = os.path.join(REPO_ROOT, "logs")
LOCKFILE = os.path.join(REPO_ROOT, "inventorysync.lock")
BACKFILL_LOCKFILE = os.path.join(REPO_ROOT, "backfill.lock")

# -------------------------------------------------
# Timeouts
# -------------------------------------------------
LOCK_TIMEOUT_SECONDS = int(os.getenv("INVENTORY_SYNC_LOCK_TIMEOUT_SECONDS", str(6 * 3600)))
WAIT_FOR_BACKFILL_SECONDS = int(os.getenv("INVENTORY_WAIT_FOR_BACKFILL_SECONDS", "120"))

# -------------------------------------------------
# Database
# -------------------------------------------------
SQLSERVER_CONNECTION_STRING = os.getenv("SQLSERVER_CONNECTION_STRING")

# -------------------------------------------------
# LWA & SP-API
# -------------------------------------------------
LWA_TOKEN_URL = os.getenv("LWA_TOKEN_URL", "https://api.amazon.com/auth/o2/token")
LWA_CLIENT_ID = os.getenv("LWA_CLIENT_ID")
LWA_CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
LWA_REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN")
SPAPI_ENDPOINT = os.getenv("SPAPI_ENDPOINT")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID", "A2VIGQ35RCS4UG")
SELLER_ID = os.getenv("SELLER_ID")

# -------------------------------------------------
# Base Currency & Fees
# -------------------------------------------------
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "AED")
FEES_ESTIMATE_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.05"))
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "10"))

# -------------------------------------------------
# Debug Print
# -------------------------------------------------
if __name__ == "__main__":
    print("Checking System Environment Variables:")
    print(f"SYNC_OVERLAP_HOURS: {os.getenv('SYNC_OVERLAP_HOURS')}")
    print(f"BACKFILL_DAYS: {os.getenv('BACKFILL_DAYS')}")
    print(f"UTC_OFFSET: {os.getenv('UTC_OFFSET')}")
