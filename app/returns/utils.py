import time
import threading
from datetime import datetime, timezone, timedelta

# ---------------------------------------------------------
# Timezone helpers
# ---------------------------------------------------------

UTC_PLUS_4 = timezone(timedelta(hours=4))


def to_utc_plus_4_naive(value: str):
    """
    Convert Amazon's UTC Z timestamp into a naive datetime in UTC+4.
    Example output: 2026-02-24 14:15:34 (no timezone info).
    """
    if not value:
        return None
    try:
        dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))
        dt_utc4 = dt_utc.astimezone(UTC_PLUS_4)
        return dt_utc4.replace(tzinfo=None)
    except Exception:
        return None


def now_utc_plus_4():
    """Get current time as naive datetime in UTC+4."""
    dt = datetime.now(timezone.utc) + timedelta(hours=4)
    return dt.replace(tzinfo=None)


# ---------------------------------------------------------
# Sanitizers
# ---------------------------------------------------------

def clean_str(x):
    """Sanitize string input."""
    if x is None:
        return None
    x = str(x).strip()
    return x if x != "" else None


def safe_int(x):
    """Convert to int, return 0 on failure."""
    try:
        x = str(x).strip()
        if x in ("", " ", "-", "--", "N/A", "NA", "None", "null"):
            return 0
        return int(float(x))
    except:
        return 0


def safe_float(x):
    """Convert to float, return 0.0 on failure."""
    try:
        x = str(x).strip()
        if x in ("", " ", "-", "--", "N/A", "NA", "None", "null"):
            return 0.0
        return float(x)
    except:
        return 0.0


def safe_dt(x):
    """
    Convert ISO8601 → UTC+4 naive datetime.
    Handles both timezone-aware and naive inputs.
    """
    if not x:
        return None

    x = str(x).strip()
    if x in ("", " ", "N/A", "NA", "-", "--", "0000-00-00T00:00:00+00:00"):
        return None

    try:
        if x.endswith("Z"):
            x = x.replace("Z", "+00:00")

        dt_utc = datetime.fromisoformat(x)
        if dt_utc.tzinfo is None:
            dt_utc = dt_utc.replace(tzinfo=timezone.utc)

        dt_utc4 = dt_utc.astimezone(UTC_PLUS_4)
        return dt_utc4.replace(tzinfo=None)

    except:
        return None
