def bulk_insert_inventory_ledger(cursor, rows):

    sql = """
    INSERT INTO InventoryLedger (
        LedgerDate,
        LedgerDateTime,

        FNSKU,
        ASIN,
        SKU,

        Title,

        EventType,
        ReferenceID,

        Quantity,

        FulfillmentCenter,

        Disposition,
        Reason,

        Country,

        ReconciledQuantity,
        UnreconciledQuantity
    )
    VALUES (
        ?, ?,
        ?, ?, ?,
        ?,
        ?, ?,
        ?,
        ?,
        ?, ?,
        ?,
        ?, ?
    )
    """

    data = []

    for r in rows:

        data.append((
            r["Date"],
            r["DateTime"],

            r["FNSKU"],
            r["ASIN"],
            r["SKU"],

            r["Title"],

            r["EventType"],
            r["ReferenceID"],

            r["Quantity"],

            r["FulfillmentCenter"],

            r["Disposition"],
            r["Reason"],

            r["Country"],

            r["ReconciledQuantity"],
            r["UnreconciledQuantity"]
        ))

    cursor.fast_executemany = True
    cursor.executemany(sql, data)

    return len(data)