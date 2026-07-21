"""
Inventory Ledger Database Functions

Purpose
-------
Handles SQL Server inserts for Inventory Ledger
adjustment events.

Important Notes
---------------
- Only adjustment events are stored.
- ReferenceID is treated as the adjustment business key.
- InventoryLedgerId remains the technical primary key.
- Designed to support fast_executemany bulk inserts.
"""


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

    if not data:
        return 0

    cursor.fast_executemany = True
    cursor.executemany(sql, data)

    return len(data)