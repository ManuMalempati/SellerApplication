# main.py
from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from .estimates import get_fees_estimate
from datetime import datetime, timedelta
import os

app = FastAPI()

connection = connect_database()

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")

@app.get("/raw-transactions")
def raw_transactions(
    days: int = 0,
    hours: int = 5,
    minutes: int = 0
):
    """
    Returns RAW Finances v2024-06-19 listTransactions response.
    No processing, no parsing, no transformations.
    """

    from datetime import datetime, timedelta

    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "postedAfter": posted_after,
        "pageSize": 100
    }

    response = spapi_request(
        method="GET",
        path="/finances/2024-06-19/transactions",
        params=params
    )

    return response

@app.get("/transactions")
async def transactions(days: int = 0, hours: int = 5, minutes: int = 0):

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

@app.get("/estimate-fees")
async def estimated_fees():

    sku = "SDSQUNR-128G-GN6MN-1"
    asin = "B07HHD7C7T"

    response = get_fees_estimate(sku=sku, asin=asin, price=48)

    return response

@app.get("/orders")
async def orders(days: int = 10, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "LastUpdatedAfter": last_updated_after,
        "MaxResultsPerPage": 100
    }

    cursor = connection.cursor()

    # Call async version directly (FastAPI handles it)
    from .orders import get_orders
    response = await get_orders(params=params, db_cursor=cursor)

    cursor.close()

    return response



if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)

from . import test
