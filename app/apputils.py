from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_result

# ---------------------------------------------------------
# Retry Logic
# ---------------------------------------------------------

def _should_retry(result):
    if not isinstance(result, dict):
        return False

    errors = result.get("errors")
    if not errors:
        return False

    retryable = {"QuotaExceeded", "RequestThrottled"}
    return any(e.get("code") in retryable for e in errors)


@retry(
    retry=retry_if_result(_should_retry),
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=5, min=5),
)
def retry_call(func, *args, **kwargs):
    return func(*args, **kwargs)