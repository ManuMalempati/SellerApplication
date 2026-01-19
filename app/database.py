import pyodbc
import os
from dotenv import load_dotenv

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