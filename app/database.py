#!/usr/bin/env python3
import pyodbc
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import json
import time

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
            LEFT JOIN InventoryReport ir ON pm.ssku = ir.PartNumber
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

# -------------- DELETE & REPLACE ORDER ITEMS --------------

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
            ?, ?, ?, ?, ?, ?,
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
    # 1. Delete existing rows for this order
    cursor.execute("DELETE FROM OrderItems WHERE AmazonOrderId = ?", (amazon_order_id,))

    if not rows:
        return

    # 2. Prepare bulk insert
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
            ?, ?, ?, ?, ?, ?,
            ?, ?, ?, ?, ?,
            ?, ?
        )
    """

    params = []
    for row in rows:
        params.append((
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
        ))

    # 3. Bulk insert
    cursor.executemany(sql, params)

def bulk_upsert_fba_data(cursor, fba_rows):
    """
    Optimized bulk upsert with batching.
    Processes in chunks of 500 rows to avoid timeouts.
    """
    start = time.time()
    total = len(fba_rows)
    print(f"[bulk_upsert_fba_data] Starting upsert of {total} rows...")

    # Pre-check: which SKUs already exist?
    cursor.execute("SELECT sku FROM ProductMapping")
    existing_skus = {row[0] for row in cursor.fetchall()}
    print(f"[bulk_upsert_fba_data] Found {len(existing_skus)} existing SKUs in database")

    # Split rows into UPDATE vs INSERT
    update_params = []
    insert_params = []

    for row in fba_rows:
        sku = row.get("SKU")
        if not sku:
            continue

        params = (
            row.get("ASIN"),
            row.get("FNSKU"),
            row.get("FBA-Stock"),
            row.get("Sellable-Qty"),
            row.get("Unsellable-Qty"),
            row.get("Condition-Type"),
            row.get("Warehouse-Condition"),
            row.get("Title"),
            row.get("COG"),
            row.get("Brand"),
            row.get("Category"),
            row.get("TotalOrderItems_L30"),
            row.get("OrderedProductSales_L30"),
            row.get("UnitsRefunded_L30"),
            row.get("BuyBoxPercentage_L30"),
            row.get("Sale-Price"),
            row.get("Est-Fee"),
            row.get("Est-FBA Fee"),
            row.get("Est-VAT"),  # Using Est-VAT
            row.get("Est-Net"),
            sku,
        )

        if sku in existing_skus:
            update_params.append(params)
        else:
            insert_params.append((sku,) + params[:-1])

    # UPDATE SQL
    update_sql = """
        UPDATE ProductMapping SET
            asin = ?,
            [FNSKU] = ?,
            [FBA-Stock] = ?,
            [Sellable-Qty] = ?,
            [Unsellable-Qty] = ?,
            [Condition-Type] = ?,
            [Warehouse-Condition] = ?,
            Title = ?,
            COG = ?,
            Brand = ?,
            Category = ?,
            TotalOrderItems_L30 = ?,
            OrderedProductSales_L30 = ?,
            UnitsRefunded_L30 = ?,
            BuyBoxPercentage_L30 = ?,
            [Sale-Price] = ?,
            [Est-Fee] = ?,
            [Est-FBA Fee] = ?,
            [Est-VAT] = ?,
            [Est-Net] = ?,
            fba_updated_at = GETDATE()
        WHERE sku = ?
    """

    # INSERT SQL
    insert_sql = """
        INSERT INTO ProductMapping (
            sku, asin, [FNSKU], [FBA-Stock], [Sellable-Qty], [Unsellable-Qty],
            [Condition-Type], [Warehouse-Condition], Title, COG, Brand, Category,
            TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30,
            BuyBoxPercentage_L30, [Sale-Price], [Est-Fee], [Est-FBA Fee], [Est-VAT],
            [Est-Net], fba_updated_at
        ) VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE()
        )
    """

    BATCH_SIZE = 500

    # Execute UPDATE in batches
    if update_params:
        print(f"[bulk_upsert_fba_data] Running UPDATE for {len(update_params)} rows...")
        for i in range(0, len(update_params), BATCH_SIZE):
            batch = update_params[i:i + BATCH_SIZE]
            cursor.executemany(update_sql, batch)
            print(f"[bulk_upsert_fba_data] Updated batch {i // BATCH_SIZE + 1}: {len(batch)} rows")

    # Execute INSERT in batches
    if insert_params:
        print(f"[bulk_upsert_fba_data] Running INSERT for {len(insert_params)} rows...")
        for i in range(0, len(insert_params), BATCH_SIZE):
            batch = insert_params[i:i + BATCH_SIZE]
            cursor.executemany(insert_sql, batch)
            print(f"[bulk_upsert_fba_data] Inserted batch {i // BATCH_SIZE + 1}: {len(batch)} rows")

    elapsed = time.time() - start
    print(f"[bulk_upsert_fba_data] Completed in {elapsed:.2f}s")
    print(f"[bulk_upsert_fba_data] Updated: {len(update_params)}, Inserted: {len(insert_params)}")

    return len(update_params) + len(insert_params)