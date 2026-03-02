from fastapi import FastAPI
from .transactions.transactions import get_transactions
from .database import connect_database
from datetime import datetime, timedelta, timezone
from .orders.orders import get_orders
from .fba import fba_report  # Changed: import from new package
from .test import router as test_router
from .utils import convert_utc_to_utcz_string

app = FastAPI()
app.include_router(test_router)

@app.get("/transactions")
async def transactions(days: int = 2, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = convert_utc_to_utcz_string(datetime.now(timezone.utc) - delta)
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
async def orders(days: int = 1, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = convert_utc_to_utcz_string(datetime.now(timezone.utc) - delta)
    params = {"LastUpdatedAfter": last_updated_after, "MaxResultsPerPage": 100}

    response = await get_orders(params=params)
    return response

@app.get("/buybox")
async def buybox():
    return await fba_report()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)