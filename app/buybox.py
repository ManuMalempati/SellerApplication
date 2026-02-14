import os
import time
from .auth import spapi_request
from .database import connect_database, get_all_product_mapping

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
LAST_CALL = 0


# ---------------------------------------------------------
# Helpers
# ---------------------------------------------------------

def debug(msg):
    print(f"[DEBUG] {msg}")


def chunk_list(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def throttle():
    """Ensure at least 0.5 seconds between API calls."""
    global LAST_CALL
    now = time.time()
    elapsed = now - LAST_CALL
    if elapsed < 0.5:
        time.sleep(0.5 - elapsed)
    LAST_CALL = time.time()


# ---------------------------------------------------------
# MAIN FUNCTION
# ---------------------------------------------------------

def buyboxes():
    debug("Connecting to database...")
    conn = connect_database()
    cursor = conn.cursor()

    product_mappings = get_all_product_mapping(cursor) or {}
    all_skus = list(product_mappings.keys())

    cursor.close()
    conn.close()
    debug(f"Loaded {len(all_skus)} SKUs from ProductMapping")

    rows = []
    all_summaries = []

    # -----------------------------------------------------
    # Fetch inventory summaries in batches of 50 SKUs
    # -----------------------------------------------------
    for batch in chunk_list(all_skus, 50):
        debug(f"Requesting inventory summaries for batch of {len(batch)} SKUs")
        throttle()

        params = {
            "granularityType": "Marketplace",
            "granularityId": MARKETPLACE_ID,
            "marketplaceIds": [MARKETPLACE_ID],
            "sellerSkus": batch,
            "details": True
        }

        response = spapi_request(
            method="GET",
            path="/fba/inventory/v1/summaries",
            params=params
        ) or {}

        payload = response.get("payload", {})
        summaries = payload.get("inventorySummaries", [])
        debug(f" → Received {len(summaries)} summaries")

        all_summaries.extend(summaries)

        # -------------------------------------------------
        # Pagination
        # -------------------------------------------------
        next_token = response.get("pagination", {}).get("nextToken")

        while next_token:
            debug(f"Paginating with nextToken: {next_token}")
            throttle()

            response = spapi_request(
                method="GET",
                path="/fba/inventory/v1/summaries",
                params={
                    "nextToken": next_token,
                    "granularityType": "Marketplace",
                    "granularityId": MARKETPLACE_ID,
                    "marketplaceIds": [MARKETPLACE_ID],
                    "details": True
                }
            ) or {}

            payload = response.get("payload", {})
            summaries = payload.get("inventorySummaries", [])
            debug(f" → Pagination returned {len(summaries)} summaries")

            all_summaries.extend(summaries)
            next_token = response.get("pagination", {}).get("nextToken")

    debug(f"Total summaries collected: {len(all_summaries)}")

    # -----------------------------------------------------
    # Build rows using ONLY totalQuantity
    # -----------------------------------------------------
    for summary in all_summaries:
        total_qty = summary.get("totalQuantity", 0)

        # For now keep >= 0
        if total_qty >= 0:
            sku = summary.get("sellerSku")
            mapping = product_mappings.get(sku) or {}

            row = {
                "asin": summary.get("asin"),
                "sku": sku,
                "ssku": mapping.get("ssku"),
                "fnsku": summary.get("fnSku"),
                "FBA-Stock": total_qty
            }

            debug(f"Adding SKU {sku} with totalQuantity={total_qty}")
            rows.append(row)

    debug(f"Final item count: {len(rows)}")
    return rows
