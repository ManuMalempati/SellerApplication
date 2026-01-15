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

def get_product_cost_with_sku(cursor, seller_sku):
    def parse_cost(cost_str):
        try:
            return float(cost_str.replace("$", "").replace(",", "").strip())
        except:
            return None

    # 1. Try exact match
    query = """
        SELECT Cost
        FROM InventoryReport
        WHERE PartNumber = ?
    """
    cursor.execute(query, (seller_sku,))
    row = cursor.fetchone()

    if row:
        return [parse_cost(row[0]), seller_sku]

    # 2. Try stripped SKU
    stripped = strip_suffix(seller_sku)
    cursor.execute(query, (stripped,))
    row = cursor.fetchone()

    if row:
        return [parse_cost(row[0]), stripped]

    # 3. Nothing found
    return [None, stripped]