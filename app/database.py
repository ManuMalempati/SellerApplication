import pyodbc
import os
from dotenv import load_dotenv


load_dotenv()

region_codes = os.getenv("REGION_CODES")

def connect_database():
    try:
        connection = pyodbc.connect(os.getenv("SQLSERVER_CONNECTION_STRING"))
        print("Database Connection successful")
        return connection
    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if(sqlstate == '28000'):
            print(f"Authentication error: {e.args}")
        else:
            print(f"Connection failed: {sqlstate}")

def strip_suffix(sku):

    if "-" in sku:
        base, suffix = sku.rsplit("-", 1)
        if suffix.isdigit():
            return base

    for region in region_codes:
        if(sku.endswith(region)):
            return sku[:-len(region)]
    
    return sku

def get_all_product_costs(cursor, asin_list):
    """Fetch all costs in one query"""

    asin_to_cost = {}
    asin_to_ssku = {}

    if not asin_list:
        return asin_to_cost, asin_to_ssku
    
    unique_asins = list(set(asin_list))
    placeholders = ','.join('?' * len(unique_asins))
    
    # here we are retrieving the cost for every ASIN -> SSKU -> PartNumber that is needed
    query = f"""
        SELECT pm.asin, pm.ssku, ir.Cost
        FROM ProductMapping pm
        LEFT JOIN InventoryReport ir ON pm.ssku = ir.PartNumber
        WHERE pm.asin IN ({placeholders})
    """

    cursor.execute(query, unique_asins)

    for row in cursor.fetchall():
        asin = row[0]
        ssku = row[1]
        cost = row[2]

        asin_to_ssku[asin] = ssku
        asin_to_cost[asin] = cost

    return asin_to_cost, asin_to_ssku


def get_asins_from_db_or_api(cursor, seller_sku_list):
    """Get ASINs - check database first, call API only if needed"""
    from .auth import spapi_request  # Import here to avoid circular import
    
    sku_to_asin = {}
    
    if not seller_sku_list:
        return sku_to_asin
    
    unique_skus = list(set(seller_sku_list))
    placeholders = ','.join('?' * len(unique_skus))
    
    # Check database first
    query = f"""
        SELECT sku, asin
        FROM ProductMapping
        WHERE sku IN ({placeholders})
    """
    cursor.execute(query, unique_skus)
    
    for row in cursor.fetchall():
        sku_to_asin[row[0]] = row[1]
    
    return sku_to_asin