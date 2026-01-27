#!/usr/bin/env python3
import pyodbc
from datetime import datetime, timezone
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
    Get SKU -> ASIN -> SSKU mapping from ProductMapping.

    Returns:
        dict: {
          sku: {
            "asin": str|None,
            "ssku": str|None,
            "last_price": None,      # kept for compatibility
            "fees": None,            # kept for compatibility
            "fee_updated_at": None,  # kept for compatibility
          }
        }
    Note: Fee-cache columns were removed from the DB; this function intentionally
    selects only sku, asin, ssku so it works regardless of those columns.
    """
    product_mapping = {}

    if not seller_sku_list:
        return product_mapping

    unique_skus = list(set(seller_sku_list))
    placeholders = ",".join("?" * len(unique_skus))

    # Select only the core mapping columns. Fee cache columns (if present) are ignored.
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
            # keep these keys for compatibility with call sites; values are None
            "last_price": None,
            "fees": None,
            "fee_updated_at": None,
        }

    return product_mapping


def get_product_details_by_asin(cursor, asin_list):
    """
    Get product details (cost, brand, category, item_name) for given ASINs.

    Returns a dict mapping asin -> {
        "cost": <raw cost from InventoryReport or None>,
        "brand": <brand or None>,
        "category": <category or None>,
        "item_name": <ItemName from InventoryReport or None>
    }
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
            ir.Category,
            ir.ItemName
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
        item_name = row[4]

        product_details[asin] = {
            "cost": cost,
            "brand": brand,
            "category": category,
            "item_name": item_name,
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
# Fee cache stubs — persistent cache removed
# -------------------------------------------------------------------

def get_fee_estimate_from_product_mapping(cursor, sku: str):
    """
    Fee cache disabled — always return None to force live fee estimate.
    Kept as a stub to avoid changing many call sites.
    """
    return None


def upsert_fee_estimate_to_product_mapping(cursor, sku: str, asin: str, price: float, fees_dict: dict):
    """
    Fee cache disabled — no-op stub.
    Kept as a stub to avoid changing many call sites.
    """
    return None