from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from .estimates import get_fees_estimate
from datetime import datetime, timedelta, timezone
from .orders import get_orders
from .buybox_report import buyboxes
from .test import router as test_router
import os

app = FastAPI()
app.include_router(test_router)

def format_dt_z(d: datetime) -> str:
    """Return canonical UTC Z timestamp like 2026-01-26T05:48:16Z."""
    if d is None:
        return None
    if d.tzinfo is None:
        return d.strftime("%Y-%m-%dT%H:%M:%SZ")
    return d.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

@app.get("/transactions")
async def transactions(days: int = 1, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = format_dt_z(datetime.now(timezone.utc) - delta)
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
    last_updated_after = format_dt_z(datetime.now(timezone.utc) - delta)
    params = {"LastUpdatedAfter": last_updated_after, "MaxResultsPerPage": 100}

    response = await get_orders(params=params)
    return response

@app.get("/buybox")
async def buybox():
    return buyboxes()


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)