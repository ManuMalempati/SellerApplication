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
    Uses ProductMapping table for lookups.
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

def get_reserved_inventory_by_ssku(cursor, ssku_list):
    """
    Get TotalStock (Reserved Inventory) from CurrentInventory for a list of SSKUs.
    """
    if not ssku_list:
        return {}
    
    unique_sskus = [s for s in set(ssku_list) if s]
    if not unique_sskus:
        return {}
    
    results = {}
    BATCH_SIZE = 1000
    
    for i in range(0, len(unique_sskus), BATCH_SIZE):
        batch = unique_sskus[i:i + BATCH_SIZE]
        placeholders = ",".join("?" * len(batch))
        
        query = f"""
            SELECT PartNumber, TotalStock
            FROM spapi_app_user.CurrentInventory
            WHERE PartNumber IN ({placeholders})
        """
        
        cursor.execute(query, batch)
        
        for row in cursor.fetchall():
            part_number = row[0]
            total_stock = row[1]
            results[part_number] = total_stock
    
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
    Optimized bulk upsert using a staging temp table and a single MERGE.
    Uses cursor.fast_executemany for high-speed bulk insert into the temp table,
    then performs a set-based MERGE to update/insert into ProductMappingTest.
    """
    start = time.time()
    total = len(fba_rows)
    print(f"[bulk_upsert_fba_data] Starting upsert of {total} rows...")

    # Deduplicate by FNSKU - keep first occurrence (same safety check as before)
    seen_fnskus = set()
    deduplicated_rows = []
    duplicate_count = 0

    for row in fba_rows:
        fnsku = row.get("FNSKU")
        if not fnsku:
            continue
        if fnsku in seen_fnskus:
            duplicate_count += 1
            continue
        seen_fnskus.add(fnsku)
        deduplicated_rows.append(row)

    if duplicate_count > 0:
        print(f"[bulk_upsert_fba_data] Removed {duplicate_count} duplicate FNSKU rows")

    # Prepare data for upload to temp table
    data_to_upload = [
        (
            row.get("FNSKU"),
            row.get("SKU"),
            row.get("ASIN"),
            row.get("SSKU"),
            row.get("FBA-Stock"),
            row.get("Sellable-Qty"),
            row.get("Unsellable-Qty"),
            row.get("Reserved-Inventory"),
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
            row.get("Est-VAT"),
            row.get("Est-Net"),
        )
        for row in deduplicated_rows
        if row.get("FNSKU")
    ]

    if not data_to_upload:
        print("[bulk_upsert_fba_data] No rows to process after deduplication.")
        return 0

    # Use fast_executemany for faster bulk insert
    try:
        cursor.fast_executemany = True
    except Exception:
        # some cursor implementations may not support this; continue without it
        pass

    # Create staging temp table
    cursor.execute("""
        CREATE TABLE #TempFBA (
            FNSKU NVARCHAR(100),
            SKU NVARCHAR(100),
            ASIN NVARCHAR(100),
            SSKU NVARCHAR(100),
            FBAStock INT,
            SellableQty INT,
            UnsellableQty INT,
            ReservedInv INT,
            Title NVARCHAR(MAX),
            COG FLOAT,
            Brand NVARCHAR(100),
            Category NVARCHAR(100),
            TotalOrderItems_L30 INT,
            OrderedProductSales_L30 FLOAT,
            UnitsRefunded_L30 INT,
            BuyBoxPercentage_L30 FLOAT,
            SalePrice FLOAT,
            EstFee FLOAT,
            EstFBAFee FLOAT,
            EstVAT FLOAT,
            EstNet FLOAT
        )
    """)

    # Bulk insert into temp table
    insert_tmp_sql = "INSERT INTO #TempFBA VALUES (" + ",".join("?" for _ in range(21)) + ")"
    cursor.executemany(insert_tmp_sql, data_to_upload)
    print(f"[bulk_upsert_fba_data] Inserted {len(data_to_upload)} rows into #TempFBA")

    # MERGE into target table (set-based upsert)
    merge_sql = """
        MERGE ProductMappingTest AS target
        USING #TempFBA AS source
        ON (target.[FNSKU] = source.FNSKU)
        WHEN MATCHED THEN
            UPDATE SET
                sku = source.SKU,
                asin = source.ASIN,
                ssku = source.SSKU,
                [FBA-Stock] = source.FBAStock,
                [Sellable-Qty] = source.SellableQty,
                [Unsellable-Qty] = source.UnsellableQty,
                [Reserved-Inventory] = source.ReservedInv,
                Title = source.Title,
                COG = source.COG,
                Brand = source.Brand,
                Category = source.Category,
                TotalOrderItems_L30 = source.TotalOrderItems_L30,
                OrderedProductSales_L30 = source.OrderedProductSales_L30,
                UnitsRefunded_L30 = source.UnitsRefunded_L30,
                BuyBoxPercentage_L30 = source.BuyBoxPercentage_L30,
                [Sale-Price] = source.SalePrice,
                [Est-Fee] = source.EstFee,
                [Est-FBA Fee] = source.EstFBAFee,
                [Est-VAT] = source.EstVAT,
                [Est-Net] = source.EstNet,
                fba_updated_at = GETDATE()
        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                [FNSKU], sku, asin, ssku, [FBA-Stock], [Sellable-Qty], [Unsellable-Qty],
                [Reserved-Inventory], Title, COG, Brand, Category,
                TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30,
                BuyBoxPercentage_L30, [Sale-Price], [Est-Fee], [Est-FBA Fee],
                [Est-VAT], [Est-Net], fba_updated_at
            )
            VALUES (
                source.FNSKU, source.SKU, source.ASIN, source.SSKU, source.FBAStock,
                source.SellableQty, source.UnsellableQty, source.ReservedInv, source.Title,
                source.COG, source.Brand, source.Category, source.TotalOrderItems_L30,
                source.OrderedProductSales_L30, source.UnitsRefunded_L30,
                source.BuyBoxPercentage_L30, source.SalePrice, source.EstFee,
                source.EstFBAFee, source.EstVAT, source.EstNet, GETDATE()
            );
    """

    cursor.execute(merge_sql)
    cursor.execute("DROP TABLE #TempFBA")

    elapsed = time.time() - start
    print(f"[bulk_upsert_fba_data] Completed in {elapsed:.2f}s")
    print(f"[bulk_upsert_fba_data] Processed: {len(data_to_upload)} rows")

    return len(data_to_upload)