# main.py
from fastapi import FastAPI
from .auth import spapi_request
from .transactions import get_transactions
from .database import connect_database
from datetime import datetime, timedelta

app = FastAPI()

connection = connect_database()

@app.get("/financial-events")
async def financial_events(days: int = 2, hours: int = 0, minutes: int = 0):

    delta = timedelta(days=days, hours=hours, minutes=minutes)

    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params={
        "PostedAfter": posted_after,
        "MaxResultsPerPage": 100
    }
    
    cursor = connection.cursor()

    filtered_data = get_transactions(params=params, db_cursor=cursor)

    cursor.close()

    # result = {"Net Profit": 0}

    # for transaction in filtered_data:
    #     result["Net Profit"] += transaction["Net Profit"]

    return filtered_data


@app.get("/raw-financial-events")
async def raw_financial_events(days: int = 3, hours: int = 0, minutes: int = 0):
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

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="127.0.0.1", port=8000)
