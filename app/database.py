#!/usr/bin/env python3
import pyodbc
from datetime import datetime, timezone, timedelta
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
    start = time.time()
    total = len(fba_rows)
    print(f"[bulk_upsert_fba_data] Starting upsert of {total} rows...")

    try:
        cursor.fast_executemany = True
    except:
        pass

    staging_rows = []
    for row in fba_rows:
        sku = row.get("SKU")
        if not sku:
            continue

        staging_rows.append((
            sku,
            row.get("ASIN"),
            row.get("FNSKU"),
            row.get("SSKU"),
            row.get("FBA-Stock") or 0,
            row.get("Sellable-Qty") or 0,
            row.get("Unsellable-Qty") or 0,
            row.get("Title"),
            row.get("COG"),
            row.get("Brand"),
            row.get("Category"),
            row.get("TotalOrderItems_L30"),
            row.get("OrderedProductSales_L30"),
            row.get("UnitsRefunded_L30"),
            row.get("BuyBoxPercentage_L30"),
            row.get("Sale-Price"),
            round(row.get("Charges") or 0, 2),
            round(row.get("Est-VAT") or 0, 2),
            round(row.get("Est-Net") or 0, 2),
            round(row.get("Profit") or 0, 2),
        ))

    if not staging_rows:
        return 0

    cursor.execute("""
        SET NOCOUNT ON;
        IF OBJECT_ID('tempdb..#TempFBA') IS NOT NULL DROP TABLE #TempFBA;
        CREATE TABLE #TempFBA (
            SKU NVARCHAR(200),
            ASIN NVARCHAR(50),
            FNSKU NVARCHAR(100),
            SSKU NVARCHAR(100),
            FBA_Stock INT,
            Sellable_Qty INT,
            Unsellable_Qty INT,
            Title NVARCHAR(1000),
            COG FLOAT,
            Brand NVARCHAR(200),
            Category NVARCHAR(200),
            TotalOrderItems_L30 INT,
            OrderedProductSales_L30 FLOAT,
            UnitsRefunded_L30 INT,
            BuyBoxPercentage_L30 FLOAT,
            Sale_Price FLOAT,
            Charges FLOAT,
            Est_VAT FLOAT,
            Est_Net FLOAT,
            Profit FLOAT
        );
    """)

    cursor.executemany("""
        INSERT INTO #TempFBA VALUES (
            ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
        );
    """, staging_rows)

    print(f"[bulk_upsert_fba_data] Bulk inserted {len(staging_rows)} rows into #TempFBA")

    merge_sql = """
        SET NOCOUNT ON;

        MERGE INTO spapi_app_user.FBAProductSummary AS target
        USING #TempFBA AS src
          ON target.FNSKU = src.FNSKU
        WHEN MATCHED THEN
            UPDATE SET
                target.sku = src.SKU,
                target.asin = src.ASIN,
                target.ssku = src.SSKU,
                target.[FBA-Stock] = src.FBA_Stock,
                target.[Sellable-Qty] = src.Sellable_Qty,
                target.[Unsellable-Qty] = src.Unsellable_Qty,
                target.Title = src.Title,
                target.COG = src.COG,
                target.Brand = src.Brand,
                target.Category = src.Category,
                target.TotalOrderItems_L30 = src.TotalOrderItems_L30,
                target.OrderedProductSales_L30 = src.OrderedProductSales_L30,
                target.UnitsRefunded_L30 = src.UnitsRefunded_L30,
                target.BuyBoxPercentage_L30 = src.BuyBoxPercentage_L30,
                target.[Sale-Price] = src.Sale_Price,
                target.Charges = src.Charges,
                target.[Est-VAT] = src.Est_VAT,
                target.[Est-Net] = src.Est_Net,
                target.Profit = src.Profit,
                target.fba_updated_at = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (sku, asin, ssku, FNSKU, [FBA-Stock], [Sellable-Qty], [Unsellable-Qty],
                    Title, COG, Brand, Category,
                    TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30, BuyBoxPercentage_L30,
                    [Sale-Price], Charges, [Est-VAT], [Est-Net], Profit, fba_updated_at)
            VALUES (src.SKU, src.ASIN, src.SSKU, src.FNSKU, src.FBA_Stock, src.Sellable_Qty, src.Unsellable_Qty,
                    src.Title, src.COG, src.Brand, src.Category,
                    src.TotalOrderItems_L30, src.OrderedProductSales_L30, src.UnitsRefunded_L30, src.BuyBoxPercentage_L30,
                    src.Sale_Price, src.Charges, src.Est_VAT, src.Est_Net, src.Profit, GETDATE())
        OUTPUT $action;

        DROP TABLE #TempFBA;
    """

    cursor.execute(merge_sql)
    actions = cursor.fetchall()

    updated = sum(1 for a in actions if a[0] == "UPDATE")
    inserted = sum(1 for a in actions if a[0] == "INSERT")

    print(f"[bulk_upsert_fba_data] Completed (Updated: {updated}, Inserted: {inserted})")

    return updated + inserted

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
