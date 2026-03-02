from app.utils import now_utc_plus_offset_naive

def bulk_upsert_fba_data(cursor, fba_rows):
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
                target.fba_updated_at = ?

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
                ?
            )

        OUTPUT $action;

        DROP TABLE #TempFBA;
    """

    cursor.execute(merge_sql, (now_utc_plus_offset_naive(), now_utc_plus_offset_naive()))
    actions = cursor.fetchall()

    updated = sum(1 for a in actions if a[0] == "UPDATE")
    inserted = sum(1 for a in actions if a[0] == "INSERT")

    print(f"[bulk_upsert_fba_data] Completed (Updated: {updated}, Inserted: {inserted})")

    return updated + inserted