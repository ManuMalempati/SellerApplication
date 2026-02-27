from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

# ---------------------------------------------------------
# Retry Logic
# ---------------------------------------------------------

def _should_retry(result):
    if isinstance(result, dict) and "errors" in result:
        codes = [e.get("code") for e in result["errors"]]
        return "QuotaExceeded" in codes or "RequestThrottled" in codes
    return False


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5),
)
def retry_call(func, *args, **kwargs):
    return func(*args, **kwargs)