from datetime import datetime, timedelta
from .main import app, connection
from .transactions import get_transactions
from .estimates import get_fees_estimate
from .auth import spapi_request
from datetime import datetime, timedelta

def fetch_all_orders(params):
    all_orders = []
    resp = spapi_request("GET", "/orders/v0/orders", params=params)
    if "errors" in resp:
        return resp  # return error payload directly

    payload = resp.get("payload") or {}
    all_orders.extend(payload.get("Orders", []))
    next_token = payload.get("NextToken")

    while next_token:
        resp = spapi_request("GET", "/orders/v0/orders", params={"NextToken": next_token})
        if "errors" in resp:
            return resp  # return the error payload
        payload = resp.get("payload") or {}
        all_orders.extend(payload.get("Orders", []))
        next_token = payload.get("NextToken")

    return {
        "count": len(all_orders),
        "orders": all_orders,
    }

@app.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100,
    }

    return fetch_all_orders(params)
