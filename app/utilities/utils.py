from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result
from datetime import datetime, timezone, timedelta
import config

# =========================================================
# Retry Logic (Only retry throttling)
# =========================================================

def _should_retry(result):
    # Batch response (list)
    if isinstance(result, list):
        for entry in result:
            err = entry.get("Error", {})
            code = err.get("Code")
            if code in {"RequestThrottled", "QuotaExceeded"}:
                return True
        return False

    # Single error dict
    if isinstance(result, dict):
        errors = result.get("errors", [])
        retryable = {"QuotaExceeded", "RequestThrottled"}
        return any(e.get("code") in retryable for e in errors)

    return False

@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5),
)
def retry_call(func, *args, **kwargs):
    return func(*args, **kwargs)

# =========================================================
# Dynamic Timezone Helpers (UTC + offset)
# =========================================================

# Build timezone dynamically (supports fractional offsets)
UTC_DYNAMIC = timezone(timedelta(hours=config.UTC_OFFSET))


def to_utc_plus_offset_naive(value: str):
    """
    Convert Amazon's UTC Z timestamp into a naive datetime in UTC+<offset>.
    Offset is read from config.UTC_OFFSET.
    """
    if not value:
        return None

    try:
        dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(UTC_DYNAMIC)
        return dt_local.replace(tzinfo=None)
    except Exception:
        return None


def now_utc_plus_offset_naive():
    """
    Current time as a naive datetime in UTC+<offset>.
    Offset is read from config.UTC_OFFSET.
    """
    dt_utc = datetime.now(timezone.utc)
    dt_local = dt_utc.astimezone(UTC_DYNAMIC)
    return dt_local.replace(tzinfo=None)

def convert_utc_to_utcz_string(dt: datetime) -> str:
    """
    Format a datetime as an ISO8601 Zulu timestamp for SP-API.
    Always outputs UTC with a trailing 'Z'.
    Use this before calling API
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return (
        dt.astimezone(timezone.utc)
          .replace(microsecond=0)
          .isoformat()
          .replace("+00:00", "Z")
    )

def get_now_iso_string_with_custom_utc_offset():
    """
    Returns a timezone-aware ISO8601 string in UTC+<offset> for logging.
    Offset is read from config.UTC_OFFSET.
    """
    dt_utc = datetime.now(timezone.utc)
    dt_local = dt_utc.astimezone(UTC_DYNAMIC)
    return dt_local.replace(microsecond=0).isoformat()

# =========================================================
# Sanitizers
# =========================================================

def clean_str(x):
    """Trim whitespace and convert empty strings to None."""
    if x is None:
        return None
    x = str(x).strip()
    return x if x else None

def safe_int(x):
    """
    Convert to int, return None on failure or placeholder values.
    Null‑preserving: only real numeric values become ints.
    """
    if x is None:
        return None
    try:
        x = str(x).strip()
        if x in ("", " ", "-", "--", "N/A", "NA", "None", "null"):
            return None
        return int(float(x))
    except:
        return None


def safe_float(x):
    """
    Convert to float, return None on failure or placeholder values.
    Null‑preserving: only real numeric values become floats.
    """
    if x is None:
        return None
    try:
        x = str(x).strip()
        if x in ("", " ", "-", "--", "N/A", "NA", "None", "null"):
            return None
        return float(x)
    except:
        return None

def safe_dt(x):
    """
    Convert ISO8601 → naive datetime in UTC+<offset>.
    Handles both timezone-aware and naive inputs.
    Uses the same dynamic offset as the rest of the ingestion pipeline.
    """
    if not x:
        return None

    x = str(x).strip()
    if x in ("", " ", "N/A", "NA", "-", "--", "0000-00-00T00:00:00+00:00"):
        return None

    try:
        # Normalize Z suffix
        if x.endswith("Z"):
            x = x.replace("Z", "+00:00")

        dt_utc = datetime.fromisoformat(x)

        # If naive, assume UTC
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)

        # Convert to UTC+offset
        dt_local = dt_utc.astimezone(UTC_DYNAMIC)

        # Return naive
        return dt_local.replace(tzinfo=None)

    except:
        return None