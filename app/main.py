# main.py
from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from datetime import datetime, timedelta

app = FastAPI()

connection = connect_database()

@app.get("/financial-events")
async def financial_events():
    posted_after = (datetime.utcnow() - timedelta(hours=3)).isoformat() + "Z"

    params={
        "PostedAfter": posted_after,
        "MaxResultsPerPage": 100
    }
    
    cursor = connection.cursor()

    filtered_data = get_transactions(params=params, db_cursor=cursor)

    cursor.close()

    return filtered_data


@app.get("/raw-financial-events")
async def raw_financial_events():
    posted_after = (datetime.utcnow() - timedelta(hours=2)).isoformat() + "Z"

    all_data = spapi_request(
        method="GET",
        path="/finances/v0/financialEvents",
        params={
            "PostedAfter": posted_after,
            "MaxResultsPerPage": 100
        })

    return all_data

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
