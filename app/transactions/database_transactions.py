from app.database import connect_database, retry_deadlock
from app.utilities.utils import now_utc_plus_offset_naive

STATUS_RANK = {
    "DEFERRED": 1,
    "DEFERRED_RELEASED": 2,
    "RELEASED": 3,
}

def update_orderitems_from_temp_financial(cur):
    """
    Updated Model B (simplified):
    - Shipments: update ALL matching rows (DEFERRED + RELEASED)
    - Refunds:   update ALL matching rows (DEFERRED + RELEASED)
    - DEFERRED_RELEASED ignored for OrderItems
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

    Identity for lifecycle = (AmazonOrderId, TransactionType, SellerSKU)

    Rules:
    - Aggregate BEFORE inserting by (OrderId, Type, SKU), keeping ONLY the HIGHEST status:
        DEFERRED < DEFERRED_RELEASED < RELEASED
    - Within that highest status, aggregate all item-level amounts.
    - No TransactionId stored.
    - At most ONE row per (OrderId, Type, SKU) in #TempFinancial, with its highest status.
    - When a higher status appears in the batch, delete lower statuses already in DB.
    - Also updates OrderItems.Payment / Refund / RefundDate (Model B).
    """

    if not rows:
        return 0

    def _do():
        conn = connect_database()
        cur = conn.cursor()
        conn.autocommit = False

        try:
            # ---------------------------------------------------------
            # 1. Aggregate by (OrderId, Type, SKU) with status hierarchy
            # ---------------------------------------------------------
            aggregated = {}

            for r in rows:
                base_key = (
                    r["AmazonOrderId"],
                    r["TransactionType"],
                    r["SellerSKU"],
                )

                status = r["TransactionStatus"]
                rank = STATUS_RANK.get(status, 0)

                existing = aggregated.get(base_key)

                if not existing or rank > STATUS_RANK.get(existing["TransactionStatus"], 0):
                    aggregated[base_key] = {
                        "PostedDate": r["PostedDate"],
                        "AmazonOrderId": r["AmazonOrderId"],
                        "TransactionType": r["TransactionType"],
                        "TransactionStatus": status,
                        "SellerSKU": r["SellerSKU"],
                        "ASIN": r["ASIN"],
                        "SSKU": r["SSKU"],

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

                agg = aggregated[base_key]
                if status == agg["TransactionStatus"]:
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

            agg_rows = list(aggregated.values())
            if not agg_rows:
                conn.commit()
                return 0

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

            insert_temp = []
            for r in agg_rows:
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
            # 3. Lifecycle delete
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
            # 4. Idempotency delete
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
                ?, ?
            FROM #TempFinancial
            """, (now_utc_plus_offset_naive(), now_utc_plus_offset_naive()))

            # ---------------------------------------------------------
            # 6. Update OrderItems (Model B)
            # ---------------------------------------------------------
            update_orderitems_from_temp_financial(cur)

            conn.commit()
            return len(agg_rows)

        except Exception as exc:
            conn.rollback()
            print("ERROR during upsert_financial_transactions:", exc)
            raise

        finally:
            cur.close()
            conn.close()

    return retry_deadlock(_do, label="FinancialTransactions")