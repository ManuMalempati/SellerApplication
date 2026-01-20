from datetime import datetime, timedelta
from .main import app, connection
from .transactions import get_transactions
from .estimates import get_fees_estimate
from .auth import spapi_request

@app.get("/fee-differences")
async def fee_differences(days: int = 2, hours: int = 0, minutes: int = 0):

    delta = timedelta(days=days, hours=hours, minutes=minutes)
    posted_after = (datetime.utcnow() - delta).isoformat() + "Z"

    params = {
        "PostedAfter": posted_after,
        "MaxResultsPerPage": 100,
    }

    cursor = connection.cursor()
    transactions = get_transactions(params=params, db_cursor=cursor)
    cursor.close()

    if not transactions:
        return {"message": "No transactions found"}

    mismatches = []
    total_checked = 0
    total_matches = 0

    for tx in transactions:
        sku = tx.get("SKU")
        asin = tx.get("ASIN")
        sold_price = tx.get("SOLD")
        actual_total_fees = abs(tx.get("TotalAmazonFees", 0))

        if not sku or not asin or sold_price is None:
            continue

        total_checked += 1

        # Use combined estimator
        estimate = get_fees_estimate(sku=sku, asin=asin, price=sold_price)
        if not estimate:
            continue

        estimated_total_fees = estimate["TotalAmazonFees"]

        if round(actual_total_fees, 2) == round(estimated_total_fees, 2):
            total_matches += 1
        else:
            mismatches.append({
                "SKU": sku,
                "ASIN": asin,
                "SSKU": tx.get("SSKU"),
                "SoldPrice": sold_price,
                "ActualFees": actual_total_fees,
                "EstimatedFees": estimated_total_fees,
                "Difference": round(estimated_total_fees - actual_total_fees, 2),
                "EstimatorSource": estimate.get("Source")
            })

    accuracy = (total_matches / total_checked * 100) if total_checked > 0 else 0

    return {
        "total_checked": total_checked,
        "matches": total_matches,
        "mismatches": len(mismatches),
        "accuracy_percent": round(accuracy, 2),
        "differences": mismatches
    }

@app.get("/test-price")
async def test_price():
    orderId = "403-8041702-9993167"
    # cursor = connection.cursor()
    response = spapi_request("GET", f"/orders/v0/orders/{orderId}/orderItems")
    return response
