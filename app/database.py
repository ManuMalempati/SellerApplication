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

def upsert_fba_data(cursor, fba_row):
    """
    Upsert FBA data into ProductMapping table.
    Uses SKU as the unique identifier.
    SQL Server syntax.
    """
    sku = fba_row.get("SKU")
    asin = fba_row.get("ASIN")
    fnsku = fba_row.get("FNSKU")
    
    if not sku:
        print(f"[upsert_fba_data] Skipping row with no SKU")
        return False
    
    # Check if SKU exists
    cursor.execute("SELECT sku FROM ProductMapping WHERE sku = ?", (sku,))
    exists = cursor.fetchone()
    
    if exists:
        # UPDATE existing row
        cursor.execute("""
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
        """, (
            asin,
            fnsku,
            fba_row.get("FBA-Stock"),
            fba_row.get("Sellable-Qty"),
            fba_row.get("Unsellable-Qty"),
            fba_row.get("Condition-Type"),
            fba_row.get("Warehouse-Condition"),
            fba_row.get("Title"),
            fba_row.get("COG"),
            fba_row.get("Brand"),
            fba_row.get("Category"),
            fba_row.get("TotalOrderItems_L30"),
            fba_row.get("OrderedProductSales_L30"),
            fba_row.get("UnitsRefunded_L30"),
            fba_row.get("BuyBoxPercentage_L30"),
            fba_row.get("Sale-Price"),
            fba_row.get("Est-Fee"),
            fba_row.get("Est-FBA Fee"),
            fba_row.get("Est-VAT"),
            fba_row.get("Est-Net"),
            sku
        ))
    else:
        # INSERT new row (SSKU will be NULL for new FBA items not in ProductMapping)
        cursor.execute("""
            INSERT INTO ProductMapping (
                sku, ssku, asin, [FNSKU], [FBA-Stock], [Sellable-Qty], [Unsellable-Qty],
                [Condition-Type], [Warehouse-Condition], Title, COG, Brand, Category,
                TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30,
                BuyBoxPercentage_L30, [Sale-Price], [Est-Fee], [Est-FBA Fee], [Est-VAT],
                [Est-Net], fba_updated_at
            ) VALUES (
                ?, NULL, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, GETDATE()
            )
        """, (
            sku,
            asin,
            fnsku,
            fba_row.get("FBA-Stock"),
            fba_row.get("Sellable-Qty"),
            fba_row.get("Unsellable-Qty"),
            fba_row.get("Condition-Type"),
            fba_row.get("Warehouse-Condition"),
            fba_row.get("Title"),
            fba_row.get("COG"),
            fba_row.get("Brand"),
            fba_row.get("Category"),
            fba_row.get("TotalOrderItems_L30"),
            fba_row.get("OrderedProductSales_L30"),
            fba_row.get("UnitsRefunded_L30"),
            fba_row.get("BuyBoxPercentage_L30"),
            fba_row.get("Sale-Price"),
            fba_row.get("Est-Fee"),
            fba_row.get("Est-FBA Fee"),
            fba_row.get("Est-VAT"),
            fba_row.get("Est-Net")
        ))
    
    return True


def bulk_upsert_fba_data(cursor, fba_rows):
    """
    Bulk upsert FBA data into ProductMapping table.
    Returns count of successful upserts.
    """
    success_count = 0
    for row in fba_rows:
        try:
            if upsert_fba_data(cursor, row):
                success_count += 1
        except Exception as e:
            print(f"[bulk_upsert_fba_data] Error upserting SKU {row.get('SKU')}: {e}")
    return success_count
