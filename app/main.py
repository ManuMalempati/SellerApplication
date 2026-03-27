from fastapi import FastAPI
from app.transactions.transactions import get_transactions
from app.database import connect_database
from datetime import datetime, timedelta, timezone
from app.orders.orders import get_orders
from app.fba import fba_report  # Changed: import from new package
from app.test import router as test_router
from app.utilities.utils import convert_utc_to_utcz_string

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
async def orders(days: int = 7, hours: int = 0, minutes: int = 0):
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    last_updated_after = convert_utc_to_utcz_string(datetime.now(timezone.utc) - delta)
    params = {"LastUpdatedAfter": last_updated_after, "MaxResultsPerPage": 100}

    response = await get_orders(params=params)
    return response

@app.get("/buybox")
async def buybox():
    return await fba_report()


"""
    Async/await for my understanding: Allows for asynchronous operations instead of tasks executing one after
    the other even when they are waiting for an extrnal process to complete such as fetching API. It is used
    when calling APIs, databases, etc. Async function when called normally returns a coroutine which means
    it is a form of a function that can be paused, executed anytime. when we do asyncio.run(main()) it takes
    the coroutine and assigns it to the event handler (python has an inbuilt event handler) which determines
    when the coroutine must run, pause, etc.

    async def task1():
        print("task1 start")
        await asyncio.sleep(3)
        print("task1 done")

    async def task2():
        print("task2 start")
        await asyncio.sleep(5)
        print("task2 done")

    async def main():
        await asyncio.gather(task1(), task2())

    asyncio.run(main())

    The asyncio.gather(task1(), task2()) basically hands the coroutines over to event handler. The event handler
    runs task1, and at the line where it says await asyncio.sleep(3), eventhandler pauses this coroutine and
    runs task2, under task2, program reaches the asyncio.sleep(5), hence eventhandler also waits on this function
    and checks to run any other functions. Whilst waiting, the event handler is listening for any responses from
    the coroutines, and in this case task1 wakes up after 3 seconds and so it resumes the coroutine from here.
"""

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)