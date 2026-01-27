#!/usr/bin/env python3
"""
test_sync_single_order.py

Test helper: fetch a single Amazon order (by orderId), enrich it the same way
sync_orders_live does (prices, fees, product mapping) and optionally upsert the
resulting order-item rows into the DB.

Usage:
  python scripts/test_sync_single_order.py --order-id <AMAZON_ORDER_ID> [--dry-run] [--verbose]

--dry-run : do not write to DB, just print the rows prepared for upsert.
--verbose : more console output for debugging.
"""
from __future__ import annotations
import argparse
import os
import sys
import time
import datetime as dt
from typing import List, Dict, Any, Tuple, Optional
from dotenv import load_dotenv

# repo root importability
REPO_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if REPO_ROOT not in sys.path:
    sys.path.insert(0, REPO_ROOT)

ENV_PATH = os.path.join(REPO_ROOT, ".env")
if os.path.exists(ENV_PATH):
    load_dotenv(ENV_PATH)

# project imports (use same helpers as other scripts)
from app.auth import spapi_request
from app.database import (
    connect_database,
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost,
)
from app.estimates import get_fees_estimate
from app.orders import get_listing_prices_batch  # reuse pricing batch helper
import pyodbc
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("test_sync_single_order")

SQL_CS = os.getenv("SQLSERVER_CONNECTION_STRING")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
# Fee multiplier and VAT config
FEES_VAT_MULTIPLIER = float(os.getenv("FEES_ESTIMATE_VAT_MULTIPLIER", "1.0"))
try:
    GOVT_VAT_RATE_DIVISOR = float(os.getenv("GOVT_VAT_RATE_DIVISOR", "21"))
    GOVT_VAT_RATE = 1.0 / GOVT_VAT_RATE_DIVISOR
except Exception:
    GOVT_VAT_RATE_DIVISOR = 21.0
    GOVT_VAT_RATE = 1.0 / GOVT_VAT_RATE_DIVISOR

# Fee cache TTL consistent with other scripts (not used for persistent cache here)
FEE_CACHE_TTL_DAYS = int(os.getenv("FEE_CACHE_TTL_DAYS", "7"))

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--order-id", required=True, help="Amazon OrderId to fetch")
    p.add_argument("--dry-run", action="store_true", help="Do not write to DB")
    p.add_argument("--verbose", action="store_true", help="Verbose logging")
    return p.parse_args()

def normalize_datetime_for_sql(s: Optional[str]):
    if not s:
        return None
    if isinstance(s, dt.datetime):
        return s.replace(tzinfo=None).isoformat(sep=" ")
    if isinstance(s, str):
        s = s.strip()
        if s.endswith("Z"):
            s = s[:-1]
        return s.replace("T", " ")
    return None

def safe_float(v):
    if v in (None, "", "Not Available"):
        return None
    try:
        return float(v)
    except Exception:
        return None

# helpers for VAT/RVAT computation
def _safe_float(x: Any) -> Optional[float]:
    try:
        if x is None:
            return None
        return float(x)
    except Exception:
        return None

def _vat_from_net(net_amount: Optional[float]) -> Optional[float]:
    if net_amount is None:
        return None
    return net_amount * GOVT_VAT_RATE

def _vat_from_gross_via_multiplier(gross_amount: Optional[float], multiplier: float) -> Optional[float]:
    if gross_amount is None or multiplier is None or multiplier == 0:
        return None
    try:
        if multiplier <= 1.0:
            return None
        return gross_amount * (1.0 - (1.0 / multiplier))
    except Exception:
        return None

def _pick_first_nonnull(*vals):
    for v in vals:
        if v is not None:
            return v
    return None

# duplicated lightweight upsert helper (temp table + MERGE) - mirrors sync_orders_live
def upsert_order_items_bulk(conn: pyodbc.Connection, rows: List[Dict[str, Any]]):
    cur = conn.cursor()
    cur.execute("""
    CREATE TABLE #OrderItemsStaging (
        OrderItemKey NVARCHAR(300) NOT NULL,
        AmazonOrderId NVARCHAR(100) NULL,
        OrderItemId NVARCHAR(100) NULL,
        OrderDate DATETIME2 NULL,
        SKU NVARCHAR(120) NULL,
        ASIN NVARCHAR(64) NULL,
        SSKU NVARCHAR(120) NULL,
        Brand NVARCHAR(200) NULL,
        Category NVARCHAR(200) NULL,
        Title NVARCHAR(MAX) NULL,
        Qty INT NULL,
        UnitPrice FLOAT NULL,
        Subtotal FLOAT NULL,
        Currency NVARCHAR(10) NULL,
        OrderStatus NVARCHAR(60) NULL,
        LastUpdateDate DATETIME2 NULL,
        FeeIncl FLOAT NULL,
        FeePct FLOAT NULL,
        FBAFeesIncl FLOAT NULL,
        TotalFee FLOAT NULL,
        RVAT FLOAT NULL,
        VAT FLOAT NULL,
        COG FLOAT NULL,
        Profit FLOAT NULL
    );
    """)
    insert_sql = """
    INSERT INTO #OrderItemsStaging (
        OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
        SKU, ASIN, SSKU, Brand, Category, Title,
        Qty, UnitPrice, Subtotal, Currency,
        OrderStatus, LastUpdateDate,
        FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
        VAT, COG, Profit
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """
    params_list = []
    for r in rows:
        params_list.append((
            r["OrderItemKey"],
            r.get("AmazonOrderId"),
            r.get("OrderItemId"),
            r.get("OrderDate"),
            r.get("SKU"),
            r.get("ASIN"),
            r.get("SSKU"),
            r.get("Brand"),
            r.get("Category"),
            r.get("Title"),
            r.get("Qty"),
            r.get("UnitPrice"),
            r.get("Subtotal"),
            r.get("Currency"),
            r.get("OrderStatus"),
            r.get("LastUpdateDate"),
            r.get("FeeIncl"),
            r.get("FeePct"),
            r.get("FBAFeesIncl"),
            r.get("TotalFee"),
            r.get("RVAT"),
            r.get("VAT"),
            r.get("COG"),
            r.get("Profit"),
        ))
    try:
        cur.fast_executemany = True
    except Exception:
        pass
    cur.executemany(insert_sql, params_list)

    merge_sql = """
    MERGE INTO spapi_app_user.OrderItems AS target
    USING #OrderItemsStaging AS src
      ON target.OrderItemKey = src.OrderItemKey
    WHEN MATCHED THEN
      UPDATE SET
        AmazonOrderId = src.AmazonOrderId,
        OrderItemId = src.OrderItemId,
        OrderDate = src.OrderDate,
        SKU = src.SKU,
        ASIN = src.ASIN,
        SSKU = src.SSKU,
        Brand = src.Brand,
        Category = src.Category,
        Title = src.Title,
        Qty = src.Qty,
        UnitPrice = src.UnitPrice,
        Subtotal = src.Subtotal,
        Currency = src.Currency,
        OrderStatus = src.OrderStatus,
        LastUpdateDate = src.LastUpdateDate,
        FeeIncl = src.FeeIncl,
        FeePct = src.FeePct,
        FBAFeesIncl = src.FBAFeesIncl,
        TotalFee = src.TotalFee,
        RVAT = src.RVAT,
        VAT = src.VAT,
        COG = src.COG,
        Profit = src.Profit,
        LastSeenAt = SYSUTCDATETIME()
    WHEN NOT MATCHED BY TARGET THEN
      INSERT (
        OrderItemKey, AmazonOrderId, OrderItemId, OrderDate,
        SKU, ASIN, SSKU, Brand, Category, Title,
        Qty, UnitPrice, Subtotal, Currency,
        OrderStatus, LastUpdateDate,
        FeeIncl, FeePct, FBAFeesIncl, TotalFee, RVAT,
        VAT, COG, Profit, FirstSeenAt, LastSeenAt
      )
      VALUES (
        src.OrderItemKey, src.AmazonOrderId, src.OrderItemId, src.OrderDate,
        src.SKU, src.ASIN, src.SSKU, src.Brand, src.Category, src.Title,
        src.Qty, src.UnitPrice, src.Subtotal, src.Currency,
        src.OrderStatus, src.LastUpdateDate,
        src.FeeIncl, src.FeePct, src.FBAFeesIncl, src.TotalFee, src.RVAT,
        src.VAT, src.COG, src.Profit, SYSUTCDATETIME(), SYSUTCDATETIME()
      );
    """
    cur.execute(merge_sql)
    cur.execute("DROP TABLE #OrderItemsStaging")

def compute_order_item_key(amazon_order_id: str, order_item_id: Optional[str], sku: Optional[str], asin: Optional[str]) -> str:
    order_item_id = (order_item_id or "").strip()
    sku = (sku or "").strip()
    asin = (asin or "").strip()
    if order_item_id and order_item_id != "0":
        return f"{amazon_order_id}:{order_item_id}"
    return f"{amazon_order_id}:0:{sku}:{asin}"

# Live fee estimator (no persistent DB cache)
def estimate_fees_live(sku: str, asin: str, price: float):
    try:
        fees = get_fees_estimate(sku, asin, price)
    except Exception as e:
        logger.exception("Fee estimate API error for %s/%s/%s: %s", sku, asin, price, e)
        return None
    if fees and isinstance(fees, dict) and "errors" not in fees:
        return fees
    return None

def main():
    args = parse_args()
    if args.verbose:
        logger.setLevel(logging.DEBUG)

    order_id = args.order_id.strip()
    if not order_id:
        logger.error("order-id is required")
        return 2

    # 1) fetch order
    logger.info("Fetching order %s", order_id)
    order_resp = spapi_request("GET", f"/orders/v0/orders/{order_id}", params={})
    if "errors" in order_resp:
        logger.error("Error fetching order: %s", order_resp.get("errors"))
        return 3
    order_payload = order_resp.get("payload") or order_resp

    # 2) fetch order items
    logger.info("Fetching order items for %s", order_id)
    items_resp = spapi_request("GET", f"/orders/v0/orders/{order_id}/orderItems", params={})
    if "errors" in items_resp:
        logger.error("Error fetching order items: %s", items_resp.get("errors"))
        return 4
    order_items = items_resp.get("payload", {}).get("OrderItems", []) or []

    if not order_items:
        logger.info("No items returned for order %s", order_id)
        return 0

    # 3) DB mapping + details
    conn = connect_database()
    cur = conn.cursor()
    try:
        skus = [it.get("SellerSKU") for it in order_items if it.get("SellerSKU")]
        skus = list(dict.fromkeys(skus))
        mapping = get_product_mapping(cur, skus) if skus else {}
        asins = list({m.get("asin") for m in mapping.values() if m.get("asin")})
        details = get_product_details_by_asin(cur, asins) if asins else {}
    finally:
        cur.close()
        conn.close()

    # 4) prepare rows (compute per-unit price: Subtotal/Qty preferred)
    missing_price_skus = [it.get("SellerSKU") for it in order_items if not it.get("ItemPrice") and not it.get("ListPrice") and it.get("SellerSKU")]
    fallback_prices = get_listing_prices_batch(missing_price_skus) if missing_price_skus else {}

    rows: List[Dict[str, Any]] = []
    # open DB connection for fee-calls (no persistent cache)
    conn = connect_database()
    cur = conn.cursor()
    try:
        for it in order_items:
            sku = it.get("SellerSKU")
            asin = (mapping.get(sku) or {}).get("asin") if sku else it.get("ASIN") or None
            qty = it.get("QuantityOrdered", 1)
            try:
                qty = int(qty) if qty is not None else 1
            except Exception:
                qty = 1

            # compute subtotal candidates
            subtotal_val = None
            if isinstance(it.get("SubTotal"), dict):
                subtotal_val = it.get("SubTotal", {}).get("Amount")
            elif it.get("SubTotal") is not None:
                subtotal_val = it.get("SubTotal")
            # sometimes ItemPrice.Amount is actually subtotal in your dataset; use it only if SubTotal missing
            if subtotal_val is None and isinstance(it.get("ItemPrice"), dict):
                subtotal_val = it.get("ItemPrice", {}).get("Amount")

            if subtotal_val is not None:
                try:
                    unit_price = float(subtotal_val) / (qty if qty else 1)
                except Exception:
                    unit_price = fallback_prices.get(sku, 0.0)
                subtotal = float(subtotal_val)
            else:
                per_unit = None
                if isinstance(it.get("ItemPrice"), dict):
                    per_unit = it.get("ItemPrice", {}).get("Amount")
                elif isinstance(it.get("ListPrice"), dict):
                    per_unit = it.get("ListPrice", {}).get("Amount")
                unit_price = float(per_unit) if per_unit is not None else fallback_prices.get(sku, 0.0)
                subtotal = unit_price * qty if unit_price else None

            # get product details/title from details (CurrentInventory)
            d = details.get(asin, {}) if asin else {}
            title = d.get("item_name") or d.get("title") or d.get("name") or None

            # estimate fees (live)
            fees = estimate_fees_live(sku or "", asin or "", unit_price or 0.0) if sku and asin else None

            # Debug: log raw fees and env settings if verbose
            if args.verbose:
                logger.debug("FEES_VAT_MULTIPLIER=%s GOVT_VAT_RATE_DIVISOR=%s GOVT_VAT_RATE=%s", FEES_VAT_MULTIPLIER, GOVT_VAT_RATE_DIVISOR, GOVT_VAT_RATE)
                logger.debug("Raw fees payload for SKU=%s ASIN=%s price=%s: %s", sku, asin, unit_price, fees)

            # compute VAT on item (unit_price / divisor)
            vat_amount = None
            if unit_price is not None:
                vat_amount = unit_price * GOVT_VAT_RATE  # unit_price / GOVT_VAT_RATE_DIVISOR

            # parse referral & fba amounts from fee payload (best effort)
            ref_amt = _safe_float(_pick_first_nonnull(
                fees.get("ReferralFees") if isinstance(fees, dict) else None,
                fees.get("ReferralFee") if isinstance(fees, dict) else None,
                fees.get("ReferralFee.Amount") if isinstance(fees, dict) else None,
                fees.get("ReferralFeesAmount") if isinstance(fees, dict) else None
            )) if fees else None

            fba_amt = _safe_float(_pick_first_nonnull(
                fees.get("FBAFees") if isinstance(fees, dict) else None,
                fees.get("FBAFee") if isinstance(fees, dict) else None,
                fees.get("FBAFeesAmount") if isinstance(fees, dict) else None
            )) if fees else None

            # compute RVAT: VAT on fees that is claimable
            ref_vat = None
            fba_vat = None
            if ref_amt is not None:
                if FEES_VAT_MULTIPLIER > 1.0:
                    ref_vat = _vat_from_gross_via_multiplier(ref_amt, FEES_VAT_MULTIPLIER)
                else:
                    ref_vat = _vat_from_net(ref_amt)
            if fba_amt is not None:
                if FEES_VAT_MULTIPLIER > 1.0:
                    fba_vat = _vat_from_gross_via_multiplier(fba_amt, FEES_VAT_MULTIPLIER)
                else:
                    fba_vat = _vat_from_net(fba_amt)

            total_rvat = None
            if (ref_vat is not None) or (fba_vat is not None):
                total_rvat = (ref_vat or 0.0) + (fba_vat or 0.0)

            # Debug prints for computations
            if args.verbose:
                logger.debug("Computed values for SKU=%s: unit_price=%s subtotal=%s qty=%s", sku, unit_price, subtotal, qty)
                logger.debug("Parsed fee amounts: referral=%s fba=%s", ref_amt, fba_amt)
                logger.debug("VAT on item (unit_price / %s) = %s", GOVT_VAT_RATE_DIVISOR, vat_amount)
                logger.debug("RVAT components: ref_vat=%s fba_vat=%s total_rvat=%s (multiplier=%s)", ref_vat, fba_vat, total_rvat, FEES_VAT_MULTIPLIER)

            # prepare display values (matching DB negative convention)
            fee_incl = -ref_amt if ref_amt is not None else None
            fba_incl = -fba_amt if fba_amt is not None else None
            total_fee = -((ref_amt or 0.0) + (fba_amt or 0.0)) if (ref_amt is not None or fba_amt is not None) else None
            fee_pct = None
            if ref_amt is not None and unit_price:
                try:
                    fee_pct = (ref_amt / unit_price) * 100.0
                except Exception:
                    fee_pct = None

            row = {
                "AmazonOrderId": order_payload.get("AmazonOrderId") or order_payload.get("AmazonOrderId") or order_id,
                "OrderItemId": it.get("OrderItemId") or None,
                "OrderDate": normalize_datetime_for_sql(order_payload.get("PurchaseDate") or order_payload.get("OrderDate")),
                "SKU": sku,
                "ASIN": asin,
                "SSKU": (mapping.get(sku) or {}).get("ssku") if sku else None,
                "Brand": d.get("brand") if d else None,
                "Category": d.get("category") if d else None,
                "Title": title,
                "Qty": qty,
                "UnitPrice": unit_price,
                "Subtotal": subtotal,
                "Currency": it.get("ItemPrice", {}).get("CurrencyCode") if isinstance(it.get("ItemPrice"), dict) else None,
                "OrderStatus": order_payload.get("OrderStatus"),
                "LastUpdateDate": normalize_datetime_for_sql(order_payload.get("LastUpdateDate") or order_payload.get("LastUpdatedDate")),
                "FeeIncl": fee_incl,
                "FeePct": fee_pct,
                "FBAFeesIncl": fba_incl,
                "TotalFee": total_fee,
                "RVAT": -total_rvat if total_rvat is not None else None,
                "VAT": -vat_amount if vat_amount is not None else None,
                "COG": (-parse_cost(d.get("cost"))) if d.get("cost") else None,
                "Profit": None,
            }
            row["OrderItemKey"] = compute_order_item_key(row["AmazonOrderId"] or "", row["OrderItemId"], row["SKU"], row["ASIN"])
            rows.append(row)
        conn.commit()
    finally:
        cur.close()
        conn.close()

    # 5) either print or upsert
    if args.dry_run:
        for r in rows:
            print(">>", r)
        logger.info("Dry-run complete. Prepared %d rows.", len(rows))
        return 0

    logger.info("Upserting %d rows to DB", len(rows))
    db_conn = pyodbc.connect(SQL_CS)
    db_conn.autocommit = False
    try:
        upsert_order_items_bulk(db_conn, rows)
        db_conn.commit()
        logger.info("Upsert complete (%d rows)", len(rows))
    except Exception:
        db_conn.rollback()
        logger.exception("Upsert failed")
        return 1
    finally:
        db_conn.close()

    return 0

if __name__ == "__main__":
    sys.exit(main() or 0)