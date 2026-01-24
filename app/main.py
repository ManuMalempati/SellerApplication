from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from .estimates import get_fees_estimate
from datetime import datetime, timedelta
from .orders import get_orders
from .test import router as test_router
import os

app = FastAPI()
app.include_router(test_router)

@app.get("/transactions")
async def transactions(days: int = 1, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"
    params = {"postedAfter": posted_after}
    
    conn = connect_database()
    cursor = conn.cursor()
    try:
        filtered_data = get_transactions(params=params, db_cursor=cursor)
    finally:
        cursor.close()
        conn.close()
    return filtered_data

@app.get("/orders")
async def orders(days: int = 0, hours: int = 5, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = (datetime.utcnow() - delta).isoformat() + "Z"
    params = {"LastUpdatedAfter": last_updated_after, "MaxResultsPerPage": 100}

    conn = connect_database()
    cursor = conn.cursor()
    try:
        response = await get_orders(params=params, db_cursor=cursor)
    finally:
        cursor.close()
        conn.close()
    return response

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)