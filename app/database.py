#!/usr/bin/env python3
import pyodbc
from datetime import datetime, timezone, timedelta
import os
from dotenv import load_dotenv
import json
import time
import datetime as dt

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
    import time
    start = time.time()
    total = len(fba_rows)
    print(f"[bulk_upsert_fba_data] Starting upsert of {total} rows...")

    try:
        cursor.fast_executemany = True
    except:
        pass

    # ---------- TYPE-SAFE CONVERTERS ----------
    def safe_str(x):
        return str(x) if x not in (None, "") else None

    def safe_float(x):
        try:
            return float(x) if x not in (None, "") else None
        except:
            return None

    def safe_int(x):
        try:
            return int(x) if x not in (None, "") else 0
        except:
            return 0

    # ---------- BUILD STAGING ROWS ----------
    staging_rows = []
    for row in fba_rows:
        sku = row.get("SKU")
        if not sku:
            continue

        staging_rows.append((
            safe_str(sku),
            safe_str(row.get("ASIN")),
            safe_str(row.get("FNSKU")),
            safe_str(row.get("SSKU")),
            safe_int(row.get("FBA-Stock")),
            safe_int(row.get("Sellable-Qty")),
            safe_int(row.get("Unsellable-Qty")),
            safe_str(row.get("Title")),
            safe_float(row.get("COG")),
            safe_str(row.get("Brand")),
            safe_str(row.get("Category")),
            safe_int(row.get("TotalOrderItems_L30")),
            safe_float(row.get("OrderedProductSales_L30")),
            safe_int(row.get("UnitsRefunded_L30")),
            safe_float(row.get("BuyBoxPercentage_L30")),
            safe_float(row.get("Sale-Price")),
            safe_float(row.get("Charges")),
            safe_float(row.get("Est-VAT")),
            safe_float(row.get("Est-Net")),
            safe_float(row.get("Profit")),
        ))

    if not staging_rows:
        return 0

    # ---------- CREATE TEMP TABLE ----------
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

    # ---------- MERGE INTO FINAL TABLE ----------
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
                target.fba_updated_at = DATEADD(HOUR, 4, GETUTCDATE())

        WHEN NOT MATCHED BY TARGET THEN
            INSERT (
                sku, asin, ssku, FNSKU,
                [FBA-Stock], [Sellable-Qty], [Unsellable-Qty],
                Title, COG, Brand, Category,
                TotalOrderItems_L30, OrderedProductSales_L30, UnitsRefunded_L30, BuyBoxPercentage_L30,
                [Sale-Price], Charges, [Est-VAT], [Est-Net], Profit, fba_updated_at
            )
            VALUES (
                src.SKU, src.ASIN, src.SSKU, src.FNSKU,
                src.FBA_Stock, src.Sellable_Qty, src.Unsellable_Qty,
                src.Title, src.COG, src.Brand, src.Category,
                src.TotalOrderItems_L30, src.OrderedProductSales_L30, src.UnitsRefunded_L30, src.BuyBoxPercentage_L30,
                src.Sale_Price, src.Charges, src.Est_VAT, src.Est_Net, src.Profit,
                DATEADD(HOUR, 4, GETUTCDATE())
            )

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

STATUS_RANK = {
    "DEFERRED": 0,
    "DEFERRED_RELEASED": 1,
    "RELEASED": 2,
}

def update_orderitems_from_temp_financial(cur):
    """
    Updated Model B (simplified):
    - Shipments: update ALL matching rows (DEFERRED + RELEASED)
    - Refunds:   update ALL matching rows (DEFERRED + RELEASED)
    - DEFERRED_RELEASED ignored
    """

    # Shipments → Payment
    cur.execute("""
    UPDATE O
    SET O.Payment = S.Total
    FROM OrderItems O
    JOIN #TempFinancial S
      ON O.AmazonOrderId = S.AmazonOrderId
     AND O.SKU           = S.SellerSKU
    WHERE S.TransactionType = 'Shipment'
      AND S.TransactionStatus IN ('DEFERRED', 'RELEASED');
    """)

    # Refunds → update ALL rows
    cur.execute("""
    UPDATE O
    SET 
        O.Refund     = S.Total,
        O.RefundDate = S.PostedDate
    FROM OrderItems O
    JOIN #TempFinancial S
      ON O.AmazonOrderId = S.AmazonOrderId
     AND O.SKU           = S.SellerSKU
    WHERE S.TransactionType = 'Refund'
      AND S.TransactionStatus IN ('DEFERRED', 'RELEASED');
    """)



def upsert_financial_transactions(rows):
    """
    NEW MODEL (Option A):
    Identity = (AmazonOrderId, TransactionType, SellerSKU, TransactionStatus)

    Rules:
    - Aggregate BEFORE inserting.
    - DEFERRED → DEFERRED_RELEASED → RELEASED hierarchy.
    - RELEASED replaces DEFERRED/DEFERRED_RELEASED.
    - No TransactionId stored.
    - One row per identity.
    """

    if not rows:
        return 0

    conn = connect_database()
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # ---------------------------------------------------------
        # 1. Aggregate rows by new identity
        # ---------------------------------------------------------
        aggregated = {}

        for r in rows:
            key = (
                r["AmazonOrderId"],
                r["TransactionType"],
                r["SellerSKU"],
                r["TransactionStatus"],   # ⭐ part of identity
            )

            if key not in aggregated:
                aggregated[key] = {
                    "PostedDate": r["PostedDate"],
                    "AmazonOrderId": r["AmazonOrderId"],
                    "TransactionType": r["TransactionType"],
                    "TransactionStatus": r["TransactionStatus"],
                    "SellerSKU": r["SellerSKU"],
                    "ASIN": r["ASIN"],
                    "SSKU": r["SSKU"],

                    # numeric fields start at zero
                    "QuantityShipped": 0,
                    "Principal": 0.0,
                    "ShippingCharges": 0.0,
                    "Promotions": 0.0,
                    "FBAFees": 0.0,
                    "RefundCommission": 0.0,
                    "FixedClosingFee": 0.0,
                    "VariableClosingFee": 0.0,
                    "ShippingChargeback": 0.0,
                    "RefFee": 0.0,
                    "Total": 0.0,
                }

            agg = aggregated[key]

            # SUM numeric fields
            agg["QuantityShipped"] += r["QuantityShipped"] or 0
            agg["Principal"] += r["Principal"] or 0
            agg["ShippingCharges"] += r["ShippingCharges"] or 0
            agg["Promotions"] += r["Promotions"] or 0
            agg["FBAFees"] += r["FBAFees"] or 0
            agg["RefundCommission"] += r["RefundCommission"] or 0
            agg["FixedClosingFee"] += r["FixedClosingFee"] or 0
            agg["VariableClosingFee"] += r["VariableClosingFee"] or 0
            agg["ShippingChargeback"] += r["ShippingChargeback"] or 0
            agg["RefFee"] += r["RefFee"] or 0
            agg["Total"] += r["Total"] or 0

        rows = list(aggregated.values())

        # ---------------------------------------------------------
        # 2. Temp table (NO TransactionId)
        # ---------------------------------------------------------
        cur.execute("""
        IF OBJECT_ID('tempdb..#TempFinancial') IS NOT NULL DROP TABLE #TempFinancial;

        CREATE TABLE #TempFinancial(
            PostedDate DATETIME,
            TransactionType NVARCHAR(50),
            TransactionStatus NVARCHAR(50),
            AmazonOrderId NVARCHAR(50),
            SellerSKU NVARCHAR(100),
            ASIN NVARCHAR(50),
            SSKU NVARCHAR(50),
            QuantityShipped INT,
            Principal FLOAT,
            ShippingCharges FLOAT,
            Promotions FLOAT,
            FBAFees FLOAT,
            RefundCommission FLOAT,
            FixedClosingFee FLOAT,
            VariableClosingFee FLOAT,
            ShippingChargeback FLOAT,
            RefFee FLOAT,
            Total FLOAT
        )
        """)

        # Insert aggregated rows
        insert_temp = []
        for r in rows:
            insert_temp.append((
                r["PostedDate"],
                r["TransactionType"],
                r["TransactionStatus"],
                r["AmazonOrderId"],
                r["SellerSKU"],
                r["ASIN"],
                r["SSKU"],
                r["QuantityShipped"],
                r["Principal"],
                r["ShippingCharges"],
                r["Promotions"],
                r["FBAFees"],
                r["RefundCommission"],
                r["FixedClosingFee"],
                r["VariableClosingFee"],
                r["ShippingChargeback"],
                r["RefFee"],
                r["Total"],
            ))

        cur.fast_executemany = True
        cur.executemany("""
            INSERT INTO #TempFinancial VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, insert_temp)

        # ---------------------------------------------------------
        # 3. Lifecycle delete (DEFERRED → DEFERRED_RELEASED → RELEASED)
        # ---------------------------------------------------------
        cur.execute("""
        DELETE T
        FROM spapi_app_user.FinancialTransactions T
        JOIN #TempFinancial S
          ON T.AmazonOrderId   = S.AmazonOrderId
         AND T.TransactionType = S.TransactionType
         AND T.SellerSKU       = S.SellerSKU
        WHERE
            (S.TransactionStatus = 'DEFERRED_RELEASED' AND T.TransactionStatus = 'DEFERRED')
         OR (S.TransactionStatus = 'RELEASED' AND T.TransactionStatus IN ('DEFERRED','DEFERRED_RELEASED'));
        """)

        # ---------------------------------------------------------
        # 4. Idempotency delete (same identity)
        # ---------------------------------------------------------
        cur.execute("""
        DELETE T
        FROM spapi_app_user.FinancialTransactions T
        JOIN #TempFinancial S
          ON T.AmazonOrderId      = S.AmazonOrderId
         AND T.TransactionType    = S.TransactionType
         AND T.SellerSKU          = S.SellerSKU
         AND T.TransactionStatus  = S.TransactionStatus;
        """)

        # ---------------------------------------------------------
        # 5. Insert final aggregated rows
        # ---------------------------------------------------------
        cur.execute("""
        INSERT INTO spapi_app_user.FinancialTransactions (
            PostedDate,
            TransactionType,
            TransactionStatus,
            AmazonOrderId,
            SellerSKU,
            ASIN,
            SSKU,
            QuantityShipped,
            Principal,
            ShippingCharges,
            Promotions,
            FBAFees,
            RefundCommission,
            FixedClosingFee,
            VariableClosingFee,
            ShippingChargeback,
            RefFee,
            Total,
            CreatedAt,
            UpdatedAt
        )
        SELECT
            PostedDate,
            TransactionType,
            TransactionStatus,
            AmazonOrderId,
            SellerSKU,
            ASIN,
            SSKU,
            QuantityShipped,
            Principal,
            ShippingCharges,
            Promotions,
            FBAFees,
            RefundCommission,
            FixedClosingFee,
            VariableClosingFee,
            ShippingChargeback,
            RefFee,
            Total,
            DATEADD(HOUR,4,GETUTCDATE()),
            DATEADD(HOUR,4,GETUTCDATE())
        FROM #TempFinancial
        """)

        # ---------------------------------------------------------
        # 6. Update OrderItems
        # ---------------------------------------------------------
        update_orderitems_from_temp_financial(cur)

        conn.commit()
        return len(rows)

    except Exception as exc:
        conn.rollback()
        print("ERROR during upsert_financial_transactions:", exc)
        raise

    finally:
        cur.close()
        conn.close()
