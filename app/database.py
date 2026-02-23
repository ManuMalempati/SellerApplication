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

# ---------------------------------------------------------
# FAST, LIFECYCLE-AWARE, SQL-BASED SELECTIVE DELETE UPSERT
# ---------------------------------------------------------

import datetime as dt

def _parse_posted_date(value):
    if not value:
        return None
    try:
        return dt.datetime.fromisoformat(value.replace("Z", "+00:00"))
    except:
        return None

def _safe_decimal(value):
    try:
        return float(value)
    except:
        return None

STATUS_RANK = {
    "DEFERRED": 0,
    "DEFERRED_RELEASED": 1,
    "RELEASED": 2,
}

def upsert_financial_transactions(rows):
    """
    Fully optimized, lifecycle-aware upsert for FinancialTransactions.

    - Dedup by TransactionId
    - Collapse batch by logical key + financials
    - SQL-based selective delete:
        * Identity-first match
        * Financial tie-break only when needed
        * Delete only ONE predecessor
    - Insert new rows
    """

    if not rows:
        return 0

    conn = connect_database()
    cur = conn.cursor()
    conn.autocommit = False

    try:
        # -------------------------------------------------
        # 1. Deduplicate by TransactionId
        # -------------------------------------------------
        by_tid = {}
        for r in rows:
            tid = r.get("TransactionId")
            if tid:
                by_tid[tid] = r
        rows = list(by_tid.values())

        # -------------------------------------------------
        # 2. Collapse batch by logical key + financials
        # -------------------------------------------------
        collapsed = {}

        for r in rows:
            status = r.get("TransactionStatus")
            rank = STATUS_RANK.get(status, -1)

            key = (
                r.get("AmazonOrderId"),
                r.get("TransactionType"),
                r.get("SellerSKU"),
                r.get("ASIN"),
                r.get("SSKU"),
                r.get("QuantityShipped"),
                _safe_decimal(r.get("Principal")),
                _safe_decimal(r.get("ShippingCharges")),
                _safe_decimal(r.get("ShippingChargeback")),
                _safe_decimal(r.get("RefFee")),
                _safe_decimal(r.get("Total")),
            )

            existing = collapsed.get(key)
            if not existing:
                collapsed[key] = r
            else:
                existing_rank = STATUS_RANK.get(existing.get("TransactionStatus"), -1)
                if rank > existing_rank:
                    collapsed[key] = r

        rows = list(collapsed.values())

        if not rows:
            conn.commit()
            return 0

        # -------------------------------------------------
        # 3. Create temp table
        # -------------------------------------------------
        cur.execute("""
        IF OBJECT_ID('tempdb..#TempFinancial') IS NOT NULL DROP TABLE #TempFinancial;

        CREATE TABLE #TempFinancial(
            TransactionId NVARCHAR(100) PRIMARY KEY,
            PostedDate DATETIMEOFFSET,
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
            FixedClosingFee FLOAT,
            VariableClosingFee FLOAT,
            ShippingChargeback FLOAT,
            RefFee FLOAT,
            Total FLOAT
        )
        """)

        # -------------------------------------------------
        # 4. Bulk insert into temp table
        # -------------------------------------------------
        insert_temp = []
        for row in rows:
            row["PostedDate"] = _parse_posted_date(row["PostedDate"])
            insert_temp.append((
                row["TransactionId"],
                row["PostedDate"],
                row["TransactionType"],
                row["TransactionStatus"],
                row["AmazonOrderId"],
                row["SellerSKU"],
                row["ASIN"],
                row["SSKU"],
                row["QuantityShipped"],
                _safe_decimal(row["Principal"]),
                _safe_decimal(row["ShippingCharges"]),
                _safe_decimal(row["Promotions"]),
                _safe_decimal(row["FBAFees"]),
                _safe_decimal(row["FixedClosingFee"]),
                _safe_decimal(row["VariableClosingFee"]),
                _safe_decimal(row["ShippingChargeback"]),
                _safe_decimal(row["RefFee"]),
                _safe_decimal(row["Total"]),
            ))

        cur.fast_executemany = True
        cur.executemany("""
            INSERT INTO #TempFinancial VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, insert_temp)

        # -------------------------------------------------
        # 5. SQL-BASED SELECTIVE DELETE
        # -------------------------------------------------

        # Build identity groups (OrderId, Type, SKU, ASIN, SSKU)
        cur.execute("""
            WITH IdentityMatches AS (
                SELECT 
                    AmazonOrderId, TransactionType, SellerSKU, ASIN, SSKU,
                    COUNT(*) AS Cnt
                FROM spapi_app_user.FinancialTransactions
                WHERE TransactionStatus IN ('DEFERRED','DEFERRED_RELEASED')
                GROUP BY AmazonOrderId, TransactionType, SellerSKU, ASIN, SSKU
            ),
            Candidates AS (
                SELECT TOP (1)
                    T.TransactionId
                FROM spapi_app_user.FinancialTransactions T
                JOIN #TempFinancial S
                  ON T.AmazonOrderId   = S.AmazonOrderId
                 AND T.TransactionType = S.TransactionType
                 AND T.SellerSKU       = S.SellerSKU
                 AND ISNULL(T.ASIN,'') = ISNULL(S.ASIN,'')
                 AND ISNULL(T.SSKU,'') = ISNULL(S.SSKU,'')
                JOIN IdentityMatches IM
                  ON IM.AmazonOrderId   = T.AmazonOrderId
                 AND IM.TransactionType = T.TransactionType
                 AND IM.SellerSKU       = T.SellerSKU
                 AND IM.ASIN            = T.ASIN
                 AND IM.SSKU            = T.SSKU
                WHERE 
                    T.TransactionStatus IN ('DEFERRED','DEFERRED_RELEASED')
                    AND S.TransactionStatus = 'RELEASED'
                    AND (
                        IM.Cnt = 1
                        OR (
                            IM.Cnt >= 2
                            AND T.QuantityShipped      = S.QuantityShipped
                            AND T.Principal            = S.Principal
                            AND T.ShippingCharges      = S.ShippingCharges
                            AND T.ShippingChargeback   = S.ShippingChargeback
                            AND T.RefFee               = S.RefFee
                            AND T.Total                = S.Total
                        )
                    )
            )
            DELETE FROM spapi_app_user.FinancialTransactions
            WHERE TransactionId IN (SELECT TransactionId FROM Candidates);
        """)

        # -------------------------------------------------
        # 6. Delete exact TransactionIds (idempotency)
        # -------------------------------------------------
        cur.execute("""
        DELETE T
        FROM spapi_app_user.FinancialTransactions T
        JOIN #TempFinancial S
          ON T.TransactionId = S.TransactionId
        """)

        # -------------------------------------------------
        # 7. Insert all new rows
        # -------------------------------------------------
        cur.execute("""
        INSERT INTO spapi_app_user.FinancialTransactions (
            TransactionId,
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
            FixedClosingFee,
            VariableClosingFee,
            ShippingChargeback,
            RefFee,
            Total,
            CreatedAt,
            UpdatedAt
        )
        SELECT
            TransactionId,
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
            FixedClosingFee,
            VariableClosingFee,
            ShippingChargeback,
            RefFee,
            Total,
            DATEADD(HOUR,4,SYSDATETIMEOFFSET()),
            DATEADD(HOUR,4,SYSDATETIMEOFFSET())
        FROM #TempFinancial
        """)

        conn.commit()
        return len(rows)

    except Exception as exc:
        conn.rollback()
        print("ERROR during upsert_financial_transactions:", exc)
        raise

    finally:
        cur.close()
        conn.close()