from fastapi import APIRouter
from datetime import datetime, timedelta, timezone
from .auth import spapi_request
import config
from utils import convert_utc_to_utcz_string

router = APIRouter()

@router.get("/raw-orders")
async def orders(days: int = 0, hours: int = 10, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = convert_utc_to_utcz_string(datetime.now(timezone.utc) - delta)

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    return spapi_request("GET", "/orders/v0/orders", params=params)

@router.get("/order-items")
async def get_order():
    orderId = "406-3986866-3793910"
    return spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")