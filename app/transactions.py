import json
from .database import (
    get_product_mapping,
    get_product_details_by_asin,
    parse_cost
)
from .auth import spapi_request
import os

BASE_CURRENCY_CODE = os.getenv("BASE_CURRENCY_CODE")
GOVT_VAT_RATE = 1/float(os.getenv("GOVT_VAT_RATE_DIVISOR"))

def retrieve_shipment_list(method, path, params):
    """
    Get all shipment events through pagination
    
    Args:
        method: HTTP method
        path: API endpoint path
        params: Query parameters
    
    Returns:
        list: All shipment events
    """
    all_shipment_events = []
    
    # Initial API call
    json_response = spapi_request(method=method, path=path, params=params)
    
    if "errors" in json_response:
        return all_shipment_events
    
    payload = json_response.get("payload")
    if not payload:
        return all_shipment_events
    
    # Extract shipment events from payload
    def extract_shipment_events(payload):
        events = payload.get("FinancialEvents", {})
        shipment_list = events.get("ShipmentEventList", [])
        all_shipment_events.extend(shipment_list)
    
    extract_shipment_events(payload)
    
    # Paginate through remaining results
    next_token = payload.get("NextToken")
    
    while next_token:
        json_response = spapi_request(
            method=method, 
            path=path, 
            params={"NextToken": next_token}
        )
        
        if "errors" in json_response:
            break
        
        payload = json_response.get("payload")
        if not payload:
            break
        
        extract_shipment_events(payload)
        next_token = payload.get("NextToken")
    
    return all_shipment_events

def calculate_fees(item):
    """
    Calculate all fees for a shipment item
    
    Args:
        item: Shipment item data
    
    Returns:
        dict: Fee breakdown {referral_fee, fba_fees, shipping_charge_back_fees, other_fees, total}
    """
    referral_fee = 0
    fba_fees = 0
    shipping_charge_back_fees = 0
    other_fees = 0
    
    fees = item.get("ItemFeeList") or []
    
    for fee in fees:
        fee_type = fee["FeeType"]
        amount = abs(fee["FeeAmount"]["CurrencyAmount"])
        
        # In UAE Marketplace, it is named as Commission
        if fee_type in ("ReferralFee", "Commission"):
            referral_fee += amount
        # FBA fee names vary so we check prefix
        elif fee_type.startswith("FBA"):
            fba_fees += amount
        elif fee_type.startswith("ShippingChargeback"):
            shipping_charge_back_fees += amount
        else:
            other_fees += amount
    
    # Client has instructed to ignore other_fees for now
    total_fees = referral_fee + fba_fees + shipping_charge_back_fees
    
    return {
        "referral_fee": referral_fee,
        "fba_fees": fba_fees,
        "shipping_charge_back_fees": shipping_charge_back_fees,
        "other_fees": other_fees,
        "total": total_fees
    }

def calculate_customer_charges(item):
    """
    Calculate customer charges and promotions
    
    Args:
        item: Shipment item data
    
    Returns:
        dict: {
            item_price: Item listing price (Principal),
            shipping_charge: Shipping charge,
            total_charges: Sum of all ItemChargeList,
            total_promotions: Sum of all PromotionList,
            sales_proceed: Total charges - promotions
        }
    """
    item_price = 0
    shipping_charge = 0
    total_charges = 0
    
    # Calculate all charges from ItemChargeList
    for charge in item.get("ItemChargeList", []):
        charge_type = charge["ChargeType"]
        amount = charge["ChargeAmount"]["CurrencyAmount"]
        
        # Add to total charges
        total_charges += amount
        
        # Track specific charge types
        if charge_type == "Principal":
            item_price += amount
        elif charge_type == "ShippingCharge":
            shipping_charge += amount
    
    # Calculate total promotions from PromotionList
    total_promotions = 0
    for promotion in item.get("PromotionList", []):
        promotion_amount = abs(promotion["PromotionAmount"]["CurrencyAmount"])
        total_promotions += promotion_amount
    
    # Sales Proceed = Total Charges - Promotions
    sales_proceed = total_charges - total_promotions
    
    return {
        "item_price": item_price,
        "shipping_charge": shipping_charge,
        "total_charges": total_charges,
        "total_promotions": total_promotions,
        "sales_proceed": sales_proceed
    }

def calculate_profit(currency_code, sales_proceed, fees_total, vat_amount, fees_vat, cost):
    """
    Calculate net profit
    
    Args:
        currency_code: AED or any other type of currency
        sales_proceed: Sales proceed after promotions
        fees_total: Total fees
        vat_amount: VAT amount on item price
        fees_vat: VAT on fees (can be claimed back)
        cost: Cost of goods
    
    Returns:
        float or str: Net profit or "Not Available"
    """
    if cost is None:
        return "Not Available"
    # No access to currency exchange API
    elif currency_code != BASE_CURRENCY_CODE:
        return "Currency Code Different"
    
    # Net Profit = Sales Proceed - Total Fees - Item VAT + Fees VAT - COG
    net_profit = sales_proceed - fees_total - vat_amount + fees_vat - cost
    return net_profit

def get_transactions(params, db_cursor):

    """
    Process financial events and return transaction details
    
    Args:
        params: Query parameters for API call
        db_cursor: Database cursor
    
    Returns:
        list: List of transaction dictionaries
    """
    method = "GET"
    path = "/finances/v0/financialEvents"
    
    # Step 1: Retrieve all shipment events from API
    shipment_events = retrieve_shipment_list(method, path, params)
    
    # Step 2: Extract all unique seller SKUs
    all_seller_skus = list(set([
        item["SellerSKU"] 
        for order in shipment_events 
        for item in (order.get("ShipmentItemList") or [])
    ]))
    
    # Step 3: Get complete mapping: SKU -> {ASIN, SSKU}
    product_mapping = get_product_mapping(db_cursor, all_seller_skus)
    
    # Step 4: Extract all ASINs from the mapping
    all_asins = list(set([
        mapping["asin"] 
        for mapping in product_mapping.values()
    ]))
    
    # Step 5: Get product details for all ASINs (cost, brand, category)
    asin_details = get_product_details_by_asin(db_cursor, all_asins)
    
    # Step 6: Process each transaction
    transactions = []
    
    for order in shipment_events:
        for item in order["ShipmentItemList"]:
            # Initialize transaction record
            transaction = {}
            
            # Basic identifiers
            transaction["AmazonOrderId"] = order["AmazonOrderId"]
            transaction["SKU"] = item["SellerSKU"]
            
            # Get mapping for this SKU: SKU -> ASIN -> SSKU
            sku = transaction["SKU"]
            mapping = product_mapping.get(sku, {})
            
            transaction["ASIN"] = mapping.get("asin", "Not Available")
            transaction["SSKU"] = mapping.get("ssku", "Not Available")
            
            # Get product details for this ASIN
            asin = transaction["ASIN"]
            details = asin_details.get(asin, {})
            
            transaction["Brand"] = details.get("brand", "Not Available")
            transaction["Category"] = details.get("category", "Not Available")
            
            # Calculate customer charges, promotions, and sales proceed
            charges = calculate_customer_charges(item)

            # Currency Code might differ - We mark it and leave it to user for now
            transaction["Currency"] = item["ItemChargeList"][0]["ChargeAmount"]["CurrencyCode"]
            
            # Item listing price and shipping
            # Item Price was asked to be named as SOLD
            transaction["SOLD"] = charges["item_price"]
            transaction["ShippingCharge"] = charges["shipping_charge"]
            
            # Promotions (negative value - shows discount given)
            transaction["TotalPromotions"] = -charges["total_promotions"]
            
            # Sales Proceed (what customer actually paid after promotions)
            transaction["SalesProceed"] = charges["sales_proceed"]
            
            # Calculate all fees
            fees = calculate_fees(item)

            # Referral Fee was asked to be named as Fee
            transaction["Fee"] = -fees["referral_fee"]
            transaction["FBAFees"] = -fees["fba_fees"]
            transaction["ShippingChargeback"] = -fees["shipping_charge_back_fees"]
            transaction["TotalAmazonFees"] = -fees["total"]
            
            # Calculate government VAT (% of item price, negative)
            vat_amount = charges["item_price"] * GOVT_VAT_RATE
            transaction["VAT"] = -vat_amount
            
            # Calculate VAT on fees - ONLY for Fee and FBAFees (NOT ShippingChargeback)
            # R.VAT = (Fee + FBAFees) / 21
            fees_with_vat = fees["referral_fee"] + fees["fba_fees"]
            fees_vat = fees_with_vat / (1 / GOVT_VAT_RATE)

            # R.VAT (can be claimed back from government)
            transaction["R.VAT"] = fees_vat

            # Referral Fee % without VAT
            # Extract VAT from referral fee only
            referral_vat = fees["referral_fee"] / (1 / GOVT_VAT_RATE)
            
            if charges["item_price"] != 0:
                net_referral_fee = fees["referral_fee"] - referral_vat
                transaction["Fee%"] = (net_referral_fee / charges["item_price"]) * 100
            else:
                transaction["Fee%"] = 0

            # Get and parse cost
            cost = parse_cost(details.get("cost"))
            
            # Store cost of goods (make negative to show money out)
            # ALWAYS IN AED SINCE WE FETCH FROM DB
            if cost is None:
                transaction["COG"] = "Not Available"
            else:
                transaction["COG"] = -cost
            
            # Calculate net profit
            # Net Profit = Sales Proceed - Amazon Fees - Item VAT + R.VAT - COG
            net_profit = calculate_profit(
                currency_code = transaction["Currency"],
                sales_proceed=charges["sales_proceed"],
                fees_total=fees["total"],
                vat_amount=vat_amount,
                fees_vat=fees_vat,
                cost=cost
            )
            
            transaction["Net Profit"] = net_profit
            
            transactions.append(transaction)
    
    return transactions