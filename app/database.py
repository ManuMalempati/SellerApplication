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
        return connection
    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if sqlstate == '28000':
            print(f"Authentication error: {e.args}")
        else:
            print(f"Connection failed: {sqlstate}")


def get_product_mapping(cursor, seller_sku_list):
    """
    Get complete SKU -> ASIN -> SSKU mapping (+ fee cache fields) from database

    Returns:
        dict: {
          sku: {
            asin: str,
            ssku: str,
            last_price: float|None,
            fees_json: dict|None,
            fee_updated_at: datetime|None
          }
        }
    """
    product_mapping = {}

    if not seller_sku_list:
        return product_mapping

    unique_skus = list(set(seller_sku_list))
    placeholders = ",".join("?" * len(unique_skus))

    # Include fee cache columns in the mapping
    query = f"""
        SELECT sku, asin, ssku, last_price, fees_json, fee_updated_at
        FROM ProductMapping
        WHERE sku IN ({placeholders})
    """

    cursor.execute(query, unique_skus)

    for row in cursor.fetchall():
        sku = row[0]
        asin = row[1]
        ssku = row[2]
        last_price = row[3]
        fees_json = row[4]
        fee_updated_at = row[5]

        parsed_fees = None
        if fees_json:
            try:
                parsed_fees = json.loads(fees_json)
            except Exception:
                parsed_fees = None

        product_mapping[sku] = {
            "asin": asin,
            "ssku": ssku,
            "last_price": float(last_price) if last_price is not None else None,
            "fees": parsed_fees,
            "fee_updated_at": fee_updated_at,
        }

    return product_mapping


def get_product_details_by_asin(cursor, asin_list):
    """
    Get product details (cost, brand, category) for given ASINs
    """
    product_details = {}

    if not asin_list:
        return product_details

    unique_asins = list(set(asin_list))
    placeholders = ",".join("?" * len(unique_asins))

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
    """
    if cost_value is None:
        return None

    try:
        cost_str = str(cost_value).replace("$", "").replace(",", "").strip()
        return float(cost_str)
    except (ValueError, AttributeError):
        return None


# -------------------------------------------------------------------
# Fee cache is now stored on ProductMapping (NOT FeeEstimateCache)
# Columns:
#   last_price (decimal)
#   fees_json (nvarchar(max)) - JSON string
#   fee_updated_at (datetime)
# -------------------------------------------------------------------

def get_fee_estimate_from_product_mapping(cursor, sku: str):
    """
    Retrieve cached fees for a SKU from ProductMapping.

    Returns:
        dict or None:
          {
            asin: str,
            last_price: float|None,
            fees: dict|None,
            updated_at: datetime|None
          }
    """
    query = """
        SELECT asin, last_price, fees_json, fee_updated_at
        FROM ProductMapping
        WHERE sku = ?
    """
    cursor.execute(query, (sku,))
    row = cursor.fetchone()
    if not row:
        return None

    asin, last_price, fees_json, fee_updated_at = row

    fees = None
    if fees_json:
        try:
            fees = json.loads(fees_json)
        except Exception:
            fees = None

    return {
        "asin": asin,
        "last_price": float(last_price) if last_price is not None else None,
        "fees": fees,
        "updated_at": fee_updated_at,
    }


def upsert_fee_estimate_to_product_mapping(cursor, sku: str, asin: str, price: float, fees_dict: dict):
    """
    Update ProductMapping row for this SKU with the latest fee cache.

    IMPORTANT (production safety):
    - We update ONLY last_price, fees_json, fee_updated_at.
    - We DO NOT overwrite asin/ssku mappings (your mapping is production-critical).
    """
    now = datetime.utcnow()
    fees_json = json.dumps(fees_dict)

    query = """
        UPDATE ProductMapping
        SET last_price = ?, fees_json = ?, fee_updated_at = ?
        WHERE sku = ?
    """
    cursor.execute(query, (price, fees_json, now, sku))