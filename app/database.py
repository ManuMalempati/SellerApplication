#!/usr/bin/env python3
import pyodbc
from config import SQLSERVER_CONNECTION_STRING
import time, random

# ALL THE DATETIMES BEING STORED IN THE DATABASE TABLES ARE IN UTC + OFFSET FORMAT AS DEFINED IN ENV

# ============================================================
# DB CONNECTION
# ============================================================

def connect_database():
    """Establish connection to the SQL Server database"""
    try:
        connection = pyodbc.connect(SQLSERVER_CONNECTION_STRING)
        return connection
    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if sqlstate == '28000':
            print(f"Authentication error: {e.args}")
        else:
            print(f"Connection failed: {sqlstate}")


def get_product_mapping(cursor, seller_sku_list):
    product_mapping = {}
    if not seller_sku_list:
        return product_mapping
    unique_skus = list(set(seller_sku_list))
    placeholders = ",".join("?" * len(unique_skus))
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
            "ssku": ssku,
            "last_price": None,
            "fees": None,
            "fee_updated_at": None,
        }
    return product_mapping


def get_all_product_mapping(cursor):
    query = """
        SELECT sku, asin, ssku
        FROM ProductMapping
    """
    cursor.execute(query)
    rows = cursor.fetchall()
    mapping = {}
    for row in rows:
        sku = row[0]
        asin = row[1]
        ssku = row[2]
        mapping[sku] = {
            "asin": asin,
            "ssku": ssku,
            "last_price": None,
            "fees": None,
            "fee_updated_at": None,
        }
    return mapping


def get_product_details_by_asin(cursor, asin_list):
    """
    Get product details for a list of ASINs.
    Batches queries to avoid SQL Server parameter limits.
    """
    if not asin_list:
        return {}
    
    unique_asins = list(set(asin_list))
    results = {}
    
    # SQL Server limit is ~2100 parameters, use 1000 to be safe
    BATCH_SIZE = 1000
    
    for i in range(0, len(unique_asins), BATCH_SIZE):
        batch = unique_asins[i:i + BATCH_SIZE]
        placeholders = ",".join("?" * len(batch))
        
        query = f"""
            SELECT 
                pm.asin,
                ir.Cost,
                ir.Brand,
                ir.Category,
                ir.ItemName
            FROM ProductMapping pm
            LEFT JOIN InventoryReportCopy ir ON pm.ssku = ir.PartNumber
            WHERE pm.asin IN ({placeholders})
        """
        
        cursor.execute(query, batch)
        
        for row in cursor.fetchall():
            asin = row[0]
            results[asin] = {
                "cost": row[1],
                "brand": row[2],
                "category": row[3],
                "item_name": row[4],
            }
    
    return results


def parse_cost(cost_value):
    if cost_value is None:
        return None
    try:
        cost_str = str(cost_value).replace("$", "").replace(",", "").strip()
        return float(cost_str)
    except (ValueError, AttributeError):
        return None


def get_cached_fees(cursor, fee_items):
    """
    fee_items: list of (sku, asin, price)
    Returns dict keyed by (sku, asin, price)
    """
    if not fee_items:
        return {}

    results = {}

    # Deduplicate
    unique_items = list(set(fee_items))

    # SQL Server parameter limit safe batching
    BATCH_SIZE = 1000

    for i in range(0, len(unique_items), BATCH_SIZE):
        batch = unique_items[i:i + BATCH_SIZE]

        params = []
        where_clauses = []

        for (sku, asin, price) in batch:
            where_clauses.append("(SKU = ? AND ASIN = ?)")
            params.extend([sku, asin])

        sql = f"""
            SELECT SKU, ASIN, Price,
                   ReferralFee, FBAFee, Charges, VAT, Net, COG, Profit
            FROM FeeEstimatesCache
            WHERE {" OR ".join(where_clauses)}
        """

        cursor.execute(sql, params)
        for row in cursor.fetchall():
            key = (row.SKU, row.ASIN, row.Price)
            results[key] = {
                "ReferralFee": row.ReferralFee,
                "FBAFee": row.FBAFee,
                "Charges": row.Charges,
                "VAT": row.VAT,
                "Net": row.Net,
                "COG": row.COG,
                "Profit": row.Profit,
            }

    return results

# ============================================================
# DEADLOCK RETRY HELPER
# ============================================================

def retry_deadlock(fn, max_attempts=5, label=""):
    for attempt in range(1, max_attempts + 1):
        try:
            return fn()
        except pyodbc.Error as e:
            msg = str(e)
            sqlstate = e.args[0] if e.args else ""
            if "1205" in msg or sqlstate == "40001":
                wait = random.uniform(0.05, 0.25)
                print(f"[DEADLOCK] {label} attempt {attempt}/{max_attempts}, retrying in {wait:.2f}s...")
                time.sleep(wait)
                continue
            raise
    raise RuntimeError(f"[DEADLOCK] {label} failed after {max_attempts} attempts")