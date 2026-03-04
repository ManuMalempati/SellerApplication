from tenacity import retry, stop_after_attempt, retry_if_result
from tenacity.wait import wait_base
from datetime import datetime, timezone, timedelta
import random
import config

# =========================================================
# Retry Logic (Throttling + InternalError)
# =========================================================

def _is_internal_error(resp):
    """
    Detect InternalError in all Amazon response formats:
    - dict with Error
    - dict with errors
    - list of entries
    - Error may be dict OR list
    """
    # Case 1: Response is a dict
    if isinstance(resp, dict):
        # Error may be dict or list
        err = resp.get("Error") or resp.get("errors") or None

        if isinstance(err, dict):
            code = err.get("Code") or err.get("code")
            return code == "InternalError"

        if isinstance(err, list):
            for e in err:
                code = e.get("Code") or e.get("code")
                if code == "InternalError":
                    return True

        return False

    # Case 2: Response is a list of entries
    if isinstance(resp, list):
        for entry in resp:
            err = entry.get("Error") or entry.get("errors") or None

            if isinstance(err, dict):
                code = err.get("Code") or err.get("code")
                if code == "InternalError":
                    return True

            if isinstance(err, list):
                for e in err:
                    code = e.get("Code") or e.get("code")
                    if code == "InternalError":
                        return True

        return False

    return False

def _should_retry(result):
    """
    Retry on:
      - RequestThrottled
      - QuotaExceeded
      - InternalError (Amazon fee engine crash)
    """
    # Batch response (list)
    if isinstance(result, list):
        for entry in result:
            err = entry.get("Error", {})
            code = err.get("Code")
            if code in {"RequestThrottled", "QuotaExceeded", "InternalError"}:
                return True
        return False

    # Single error dict
    if isinstance(result, dict):
        errors = result.get("errors", [])
        retryable = {"QuotaExceeded", "RequestThrottled", "InternalError"}
        return any(e.get("code") in retryable for e in errors)

    return False


class wait_mixed(wait_base):
    """
    Mixed wait strategy:
      - InternalError → short jitter (0.5–2.0s)
      - Throttling → exponential backoff (5s, 10s, 20s…)
    """
    def __call__(self, retry_state):
        result = retry_state.outcome.result()

        # InternalError → short jitter
        if _is_internal_error(result):
            return 0.5 + random.random() * 1.5

        # Throttling → exponential backoff
        attempt = retry_state.attempt_number
        return min(5 * (2 ** (attempt - 1)), 60)


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_mixed()
)
def retry_call(func, *args, **kwargs):
    return func(*args, **kwargs)


# =========================================================
# Dynamic Timezone Helpers (UTC + offset)
# =========================================================

UTC_DYNAMIC = timezone(timedelta(hours=config.UTC_OFFSET))


def to_utc_plus_offset_naive(value: str):
    if not value:
        return None
    try:
        dt_utc = datetime.fromisoformat(value.replace("Z", "+00:00"))
        dt_local = dt_utc.astimezone(UTC_DYNAMIC)
        return dt_local.replace(tzinfo=None)
    except Exception:
        return None


def now_utc_plus_offset_naive():
    dt_utc = datetime.now(timezone.utc)
    dt_local = dt_utc.astimezone(UTC_DYNAMIC)
    return dt_local.replace(tzinfo=None)


def convert_utc_to_utcz_string(dt: datetime) -> str:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (
        dt.astimezone(timezone.utc)
          .replace(microsecond=0)
          .isoformat()
          .replace("+00:00", "Z")
    )


def get_now_iso_string_with_custom_utc_offset():
    dt_utc = datetime.now(timezone.utc)
    dt_local = dt_utc.astimezone(UTC_DYNAMIC)
    return dt_local.replace(microsecond=0).isoformat()


# =========================================================
# Sanitizers (Null‑preserving)
# =========================================================

def clean_str(x):
    if x is None:
        return None
    x = str(x).strip()
    return x if x else None


def safe_int(x):
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

        dt_local = dt_utc.astimezone(UTC_DYNAMIC)
        return dt_local.replace(tzinfo=None)

    except:
        return None