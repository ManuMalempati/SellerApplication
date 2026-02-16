# File moved to fba folder

import os

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
MAX_WORKERS = int(os.getenv("MAX_WORKERS", "4"))
SELLER_ID = os.getenv("SELLER_ID")  # <-- new, required for Listings API calls

_divisor_raw = os.getenv("GOVT_VAT_RATE_DIVISOR")
if _divisor_raw:
    try:
        _divisor_val = float(_divisor_raw)
        GOVT_VAT_RATE = 1 / _divisor_val if _divisor_val != 0 else 0.0
    except ValueError:
        GOVT_VAT_RATE = 0.0
else:
    GOVT_VAT_RATE = 0.0

MAX_RETRIES = 3
INITIAL_RETRY_DELAY = 5.0
