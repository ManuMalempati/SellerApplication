#!/usr/bin/env python3
"""
estimates.py — clean production version with retry on NULL-fee cases
"""

from urllib.parse import quote
import os
from typing import Any, Dict, Optional
from .auth import spapi_request

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")

# How many times to retry when Amazon returns NULL fees
FEE_RETRY_ATTEMPTS = 2


def _safe_float(value: Any) -> Optional[float]:
    try:
        if value is None:
            return None
        return float(value)
    except Exception:
        return None


def _extract_from_response(response: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Normalize SP-API response into a structured dict:
    - Returns None if no estimate was produced
    - Returns {'errors': [...]} if API returned errors
    - Returns {'raw': response, 'net': {...}} on success
    """
    if not isinstance(response, dict):
        return None

    # If API returned errors, propagate them
    if "errors" in response:
        return {"errors": response.get("errors")}

    # Standard SP-API structure
    result_root = (
        (response.get("payload") or {}).get("FeesEstimateResult")
        or response.get("FeesEstimateResult")
        or {}
    )

    status_value = result_root.get("Status")
    if status_value and status_value != "Success":
        return None

    fees_section = (
        result_root.get("FeesEstimate")
        or response.get("FeesEstimate")
        or {}
    )

    # Extract total fees
    total_section = fees_section.get("TotalFeesEstimate") or {}
    total_fees_amount = _safe_float(total_section.get("Amount"))
    total_fees_currency = total_section.get("CurrencyCode")

    referral_fee_net = 0.0
    fba_fee_net = 0.0

    fee_detail_list = fees_section.get("FeeDetailList") or []

    if fee_detail_list:
        for fee_item in fee_detail_list:
            fee_type = (fee_item.get("FeeType") or "").lower()
            final_fee_amount = _safe_float(
                (fee_item.get("FinalFee") or {}).get("Amount")
                or (fee_item.get("FeeAmount") or {}).get("Amount")
            )
            if final_fee_amount is None:
                continue

            if "referral" in fee_type:
                referral_fee_net += final_fee_amount
            elif fee_type.startswith("fba") or "fba" in fee_type or "pick" in fee_type:
                fba_fee_net += final_fee_amount
    else:
        # Legacy fallback
        referral_fee_net = _safe_float(
            fees_section.get("ReferralFee")
            or fees_section.get("ReferralFees")
            or 0.0
        ) or 0.0

        fba_fee_net = _safe_float(
            fees_section.get("FBAFees")
            or fees_section.get("FBAFee")
            or 0.0
        ) or 0.0

    return {
        "raw": response,
        "net": {
            "CurrencyCode": total_fees_currency,
            "TotalAmazonFees": total_fees_amount,
            "ReferralFees": referral_fee_net,
            "FBAFees": fba_fee_net,
        },
    }


def _request_fees(sku: str, asin: str, price: float) -> Optional[Dict[str, Any]]:
    """
    Performs a single fee request (SKU first, then ASIN).
    Returns extracted dict or None.
    """

    # --- SKU-based request ---
    if sku:
        try:
            safe_sku = quote(sku, safe="")
            body = {
                "FeesEstimateRequest": {
                    "MarketplaceId": MARKETPLACE_ID or "",
                    "IsAmazonFulfilled": True,
                    "PriceToEstimateFees": {
                        "ListingPrice": {
                            "CurrencyCode": BASE_CURRENCY_CODE,
                            "Amount": price,
                        }
                    },
                    "Identifier": f"{sku}-estimate",
                }
            }

            resp = spapi_request(
                "POST",
                f"/products/fees/v0/listings/{safe_sku}/feesEstimate",
                body=body,
            )

            extracted = _extract_from_response(resp)
            if extracted is not None:
                return extracted

        except Exception:
            pass

    # --- ASIN-based request ---
    if asin:
        try:
            body = {
                "FeesEstimateRequest": {
                    "MarketplaceId": MARKETPLACE_ID or "",
                    "IsAmazonFulfilled": True,
                    "PriceToEstimateFees": {
                        "ListingPrice": {
                            "CurrencyCode": BASE_CURRENCY_CODE,
                            "Amount": price,
                        }
                    },
                    "Identifier": f"{asin}-estimate",
                }
            }

            resp = spapi_request(
                "POST",
                f"/products/fees/v0/items/{asin}/feesEstimate",
                body=body,
            )

            extracted = _extract_from_response(resp)
            if extracted is not None:
                return extracted

        except Exception:
            pass

    return None


def get_fees_estimate(sku: str, asin: str, price: float) -> Dict[str, Any]:
    """
    Request fees estimate from SP-API.
    Retries when Amazon returns NULL-fee results.
    """

    if price is None:
        return {"errors": [{"code": "InvalidPrice", "message": "Price is required"}]}

    attempt = 0
    last_result = None

    while attempt < FEE_RETRY_ATTEMPTS:
        attempt += 1

        result = _request_fees(sku, asin, price)
        last_result = result

        # If API returned errors → stop immediately
        if isinstance(result, dict) and "errors" in result:
            return result

        # If result is None → retry
        if result is None:
            continue

        # If fees are valid → return
        net = result.get("net") or {}
        if net.get("ReferralFees") or net.get("FBAFees"):
            return result

        # If both referral and FBA fees are zero → retry
        continue

    # After retries, return last result or error
    if last_result is None:
        return {"errors": [{"code": "NoEstimate", "message": "No fees estimate returned"}]}

    return last_result
