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

@app.get("/financial-events")
async def financial_events(days: int = 0, hours: int = 3, minutes: int = 0):

    delta = timedelta(days=days, hours=hours, minutes=minutes)

    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    # Add Query params
    params={
        "PostedAfter": posted_after,
        "MaxResultsPerPage": 100,
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

@app.get("/raw-financial-events")
async def raw_financial_events(days: int = 0, hours: int = 10, minutes: int = 0):
    """
    Get all raw financial events from Amazon API with pagination
    
    Args:
        days: Number of days to look back (default: 3)
        hours: Additional hours to look back (default: 0)
        minutes: Additional minutes to look back (default: 0)
    
    Returns:
        All raw API responses combined
    """
    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    all_data = []
    
    # Initial request
    response = spapi_request(
        method="GET",
        path="/finances/v0/financialEvents",
        params={
            "PostedAfter": posted_after,
            "MaxResultsPerPage": 100
        })
    
    # Check for errors
    if "errors" in response:
        return response
    
    all_data.append(response)
    
    # Paginate through remaining results
    payload = response.get("payload")
    next_token = payload.get("NextToken") if payload else None
    
    while next_token:
        response = spapi_request(
            method="GET",
            path="/finances/v0/financialEvents",
            params={"NextToken": next_token}
        )
        
        if "errors" in response:
            break
        
        all_data.append(response)
        
        payload = response.get("payload")
        if not payload:
            break
            
        next_token = payload.get("NextToken")
    
    return {"pages": all_data, "total_pages": len(all_data)}

@app.get("/orders")
async def orders(days: int = 0, hours: int = 1, minutes: int = 0):
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
