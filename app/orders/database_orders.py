from app.database import retry_deadlock

# -------------- DELETE & REPLACE ORDER ITEMS --------------

def replace_order_items_for_order(cursor, amazon_order_id, rows):
    def _do():
        cursor.execute("DELETE FROM OrderItems WHERE AmazonOrderId = ?", (amazon_order_id,))

        if not rows:
            return

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

        cursor.executemany(sql, params)

    retry_deadlock(_do, label=f"OrderItems({amazon_order_id})")