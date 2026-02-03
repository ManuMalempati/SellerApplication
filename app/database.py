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

def get_all_product_mapping(cursor):
    """
    Return a dict of all SKUs -> { asin, ssku } from ProductMapping.
    This is used by buybox.py to load every SKU in the system.
    """
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


def upsert_order_item(cursor, row):
    """
    Perform a MERGE-based UPSERT of a single order item row.
    Matches the exact output fields from orders.py (Option B).
    """
    merge_sql = """
        MERGE INTO OrderItems AS target
        USING (SELECT ? AS OrderItemKey) AS src
        ON target.OrderItemKey = src.OrderItemKey

        WHEN MATCHED THEN
            UPDATE SET
                AmazonOrderId = ?,
                OrderDate = ?,
                SKU = ?,
                ASIN = ?,
                SSKU = ?,
                Brand = ?,
                Category = ?,
                Title = ?,
                Qty = ?,
                UnitPrice = ?,
                Subtotal = ?,
                Currency = ?,
                OrderStatus = ?,
                LastUpdateDate = ?,
                FeeIncl = ?,
                FeePct = ?,
                FBAFeesIncl = ?,
                TotalFee = ?,
                RVAT = ?,
                VAT = ?,
                COG = ?,
                Profit = ?

        WHEN NOT MATCHED THEN
            INSERT (
                OrderItemKey, AmazonOrderId, OrderDate, SKU, ASIN, SSKU,
                Brand, Category, Title, Qty, UnitPrice, Subtotal, Currency,
                OrderStatus, LastUpdateDate, FeeIncl, FeePct, FBAFeesIncl,
                TotalFee, RVAT, VAT, COG, Profit
            )
            VALUES (
                ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
            );
        """


    params = (
            # MATCH KEY
            row.get("OrderItemKey"),

            # UPDATE fields
            row.get("AmazonOrderId"),
            row.get("OrderDate"),
            row.get("SKU"),
            row.get("ASIN"),
            row.get("SSKU"),
            row.get("Brand"),
            row.get("Category"),
            row.get("Title"),
            row.get("Qty"),
            row.get("UnitPrice"),
            row.get("Subtotal"),
            row.get("Currency"),
            row.get("OrderStatus"),
            row.get("LastUpdateDate"),
            row.get("FeeIncl"),
            row.get("FeePct"),
            row.get("FBAFeesIncl"),
            row.get("TotalFee"),
            row.get("RVAT"),
            row.get("VAT"),
            row.get("COG"),
            row.get("Profit"),

            # INSERT fields
            row.get("OrderItemKey"),
            row.get("AmazonOrderId"),
            row.get("OrderDate"),
            row.get("SKU"),
            row.get("ASIN"),
            row.get("SSKU"),
            row.get("Brand"),
            row.get("Category"),
            row.get("Title"),
            row.get("Qty"),
            row.get("UnitPrice"),
            row.get("Subtotal"),
            row.get("Currency"),
            row.get("OrderStatus"),
            row.get("LastUpdateDate"),
            row.get("FeeIncl"),
            row.get("FeePct"),
            row.get("FBAFeesIncl"),
            row.get("TotalFee"),
            row.get("RVAT"),
            row.get("VAT"),
            row.get("COG"),
            row.get("Profit"),
        )

    cursor.execute(merge_sql, params)


def robust_upsert_order_items(cursor, row):
    """
    Robust wrapper around upsert_order_item:
    - Retries once on SQL Server connection drop (08S01)
    - Returns True on success, False on failure
    """
    try:
        upsert_order_item(cursor, row)
        return True

    except pyodbc.OperationalError as exc:
        msg = str(exc)
        if "08S01" in msg:
            print("DB connection lost during upsert. Reconnecting...")
            try:
                conn = connect_database()
                new_cursor = conn.cursor()
                upsert_order_item(new_cursor, row)
                conn.commit()
                new_cursor.close()
                conn.close()
                return True
            except Exception as retry_exc:
                print("Retry upsert failed: {}".format(retry_exc))
                return False
        else:
            print("OperationalError during upsert: {}".format(exc))
            return False

    except Exception as exc:
        print("General DB upsert error: {}".format(exc))
        return False

# -------------------- DELETE-AND-REPLACE LOGIC --------------------

def insert_order_item(cursor, row):
    sql = """
        INSERT INTO OrderItems (
            AmazonOrderId, OrderDate, SKU, ASIN, SSKU,
            Brand, Category, Title, Qty, UnitPrice, Subtotal, Currency,
            OrderStatus, LastUpdateDate, FeeIncl, FeePct, FBAFeesIncl,
            TotalFee, RVAT, VAT, COG, Profit,
            Refund, RefundDate, ReturnDate, ReturnDisposition, ReturnReason,
            LicensePlateNumber, Reimbursed, ReimbDate, RemovalDate, RemovalId,
            RemovalTracking, RemovalDelivery, FirstSeenAt, LastSeenAt
        )
        VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?
        )
    """
    params = (
        row["AmazonOrderId"],
        row["OrderDate"],
        row["SKU"],
        row["ASIN"],
        row["SSKU"],
        row["Brand"],
        row["Category"],
        row["Title"],
        row["Qty"],
        row["UnitPrice"],
        row["Subtotal"],
        row["Currency"],
        row["OrderStatus"],
        row["LastUpdateDate"],
        row["FeeIncl"],
        row["FeePct"],
        row["FBAFeesIncl"],
        row["TotalFee"],
        row["RVAT"],
        row["VAT"],
        row["COG"],
        row["Profit"],
        row["Refund"],
        row["RefundDate"],
        row["ReturnDate"],
        row["ReturnDisposition"],
        row["ReturnReason"],
        row["LicensePlateNumber"],
        row["Reimbursed"],
        row["ReimbDate"],
        row["RemovalDate"],
        row["RemovalId"],
        row["RemovalTracking"],
        row["RemovalDelivery"],
        row["FirstSeenAt"],
        row["LastSeenAt"],
    )
    cursor.execute(sql, params)

def replace_order_items_for_order(cursor, amazon_order_id, rows):
    cursor.execute("DELETE FROM OrderItems WHERE AmazonOrderId = ?", (amazon_order_id,))
    for row in rows:
        insert_order_item(cursor, row)