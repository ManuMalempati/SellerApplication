#!/usr/bin/env python3
"""
estimates.py

Wrapper around SP-API Fees Estimate endpoints.

Public API:
- get_fees_estimate(sku: str, asin: str, price: float) -> dict

Return shapes:
- On success:
    {
      "raw": <full spapi response dict>,
      "net": {
        "CurrencyCode": <str or None>,
        "TotalAmazonFees": <float or None>,
        "ReferralFees": <float>,
        "FBAFees": <float>
      }
    }

- On API error:
    {"errors": [ ... ]}

- If no estimate available (Status != "Success"):
    None
"""
from urllib.parse import quote
import os
from typing import Any, Dict, Optional
from .auth import spapi_request

MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")
BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE", "USD")


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

    # If Status exists and is not Success → no estimate
    status_value = result_root.get("Status")
    if status_value and status_value != "Success":
        return None

    fees_section = (
        result_root.get("FeesEstimate")
        or response.get("FeesEstimate")
        or {}
    )

    # Extract total fees if present
    total_fees_amount = None
    total_fees_currency = None
    total_section = fees_section.get("TotalFeesEstimate") or {}
    if total_section:
        total_fees_amount = _safe_float(total_section.get("Amount"))
        total_fees_currency = total_section.get("CurrencyCode")

    # Extract per-fee details
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


def get_fees_estimate(sku: str, asin: str, price: float) -> Dict[str, Any]:
    """
    Request fees estimate from SP-API.

    Attempts:
      1) Listings (SKU) endpoint
      2) Items (ASIN) endpoint

    Returns:
      - structured dict as described above, or
      - {"errors": [...]} if API returned errors, or
      - None if no estimate was produced.
    """
    if price is None:
        return {"errors": [{"code": "InvalidPrice", "message": "Price is required for fee estimation"}]}

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

        except Exception as exc:
            return {"errors": [{"code": "RequestException", "message": str(exc)}]}

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

        except Exception as exc:
            return {"errors": [{"code": "RequestException", "message": str(exc)}]}

    # No usable estimate
    return {"errors": [{"code": "NoEstimate", "message": "No fees estimate returned from SP-API"}]}
