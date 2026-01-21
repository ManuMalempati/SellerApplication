# database.py
import pyodbc
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv
import json

load_dotenv()

def connect_database():
    """Establish connection to the SQL Server database"""
    try:
        connection = pyodbc.connect(os.getenv("SQLSERVER_CONNECTION_STRING"))
        print("Database Connection successful")
        return connection
    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if sqlstate == '28000':
            print(f"Authentication error: {e.args}")
        else:
            print(f"Connection failed: {sqlstate}")

def get_product_mapping(cursor, seller_sku_list):
    """
    Get complete SKU -> ASIN -> SSKU mapping from database
    
    Args:
        cursor: Database cursor
        seller_sku_list: List of seller SKUs
    
    Returns:
        dict: {seller_sku: {asin: asin_value, ssku: ssku_value}}
    """
    product_mapping = {}
    
    if not seller_sku_list:
        return product_mapping
    
    unique_skus = list(set(seller_sku_list))
    placeholders = ','.join('?' * len(unique_skus))
    
    query = f"""
        SELECT sku, asin, ssku
        FROM ProductMapping
        WHERE sku IN ({placeholders})
    """
    
    cursor.execute(query, unique_skus)
    
    for row in cursor.fetchall():
        sku = row[0]
        asin = row[1]
        ssku = row[2]
        
        product_mapping[sku] = {
            "asin": asin,
            "ssku": ssku
        }
    
    return product_mapping

def get_product_details_by_asin(cursor, asin_list):
    """
    Get product details (cost, brand, category) for given ASINs
    
    Args:
        cursor: Database cursor
        asin_list: List of ASINs
    
    Returns:
        dict: {asin: {cost, brand, category}}
    """
    product_details = {}
    
    if not asin_list:
        return product_details
    
    unique_asins = list(set(asin_list))
    placeholders = ','.join('?' * len(unique_asins))
    
    # Join ProductMapping with InventoryReport to get all details
    query = f"""
        SELECT 
            pm.asin, 
            ir.Cost, 
            ir.Brand, 
            ir.Category
        FROM ProductMapping pm
        LEFT JOIN InventoryReport ir ON pm.ssku = ir.PartNumber
        WHERE pm.asin IN ({placeholders})
    """
    
    cursor.execute(query, unique_asins)
    
    for row in cursor.fetchall():
        asin = row[0]
        cost = row[1]
        brand = row[2]
        category = row[3]
        
        product_details[asin] = {
            "cost": cost,
            "brand": brand,
            "category": category
        }
    
    return product_details

def parse_cost(cost_value):
    """
    Parse cost from various formats (string with $, float, etc.)
    
    Args:
        cost_value: Cost as string or float
    
    Returns:
        float or None: Parsed cost value
    """
    if cost_value is None:
        return None
    
    try:
        # Convert to string first to handle both string and float inputs
        cost_str = str(cost_value).replace("$", "").replace(",", "").strip()
        return float(cost_str)
    except (ValueError, AttributeError):
        return None


def get_fee_estimate_from_cache(cursor, sku):
    query = """
        SELECT asin, last_price, fees_json, updated_at
        FROM FeeEstimateCache
        WHERE sku = ?
    """
    cursor.execute(query, (sku,))
    row = cursor.fetchone()
    if not row:
        return None

    asin, last_price, fees_json, updated_at = row
    return {
        "asin": asin,
        "last_price": float(last_price),
        "fees": json.loads(fees_json),
        "updated_at": updated_at
    }

def upsert_fee_estimate_cache(cursor, sku, asin, price, fees_dict):
    query = """
        MERGE FeeEstimateCache AS target
        USING (SELECT ? AS sku) AS source
        ON (target.sku = source.sku)
        WHEN MATCHED THEN
            UPDATE SET asin = ?, last_price = ?, fees_json = ?, updated_at = ?
        WHEN NOT MATCHED THEN
            INSERT (sku, asin, last_price, fees_json, updated_at)
            VALUES (?, ?, ?, ?, ?);
    """
    now = datetime.utcnow()
    fees_json = json.dumps(fees_dict)

    cursor.execute(
        query,
        (
            sku,              # source.sku
            asin, price, fees_json, now,   # UPDATE
            sku, asin, price, fees_json, now  # INSERT
        )
    )
