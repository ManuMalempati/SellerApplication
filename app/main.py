# main.py
from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from .estimates import get_fees_estimate
from datetime import datetime, timedelta
from .orders import get_orders
import time
import os

app = FastAPI()

connection = connect_database()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")


@app.get("/transactions")
async def transactions(days: int = 1, hours: int = 0, minutes: int = 0):

    delta = timedelta(days=days, hours=hours, minutes=minutes)

    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    # Finances v2024-06-19
    params = {
        "postedAfter": posted_after,
    }
    
    cursor = connection.cursor()

    filtered_data = get_transactions(params=params, db_cursor=cursor)

    cursor.close()

    return filtered_data

@app.get("/orders")
async def orders(days: int = 10, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    cursor = connection.cursor()

    response = await get_orders(params=params, db_cursor=cursor)

    cursor.close()

    return response



@app.get("/financial-events/{order_id}")
async def get_financial_events(order_id: str = "407-4652432-8029148"):
    """
    Retrieves raw financial events for a given Amazon Order ID.
    Uses: GET /finances/v0/orders/{orderId}/financialEvents
    """
    path = f"/finances/v0/orders/{order_id}/financialEvents"
    
    financial_events = spapi_request("GET", path)
    
    return financial_events

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

from . import test
