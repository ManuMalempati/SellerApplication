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

def retrieve_transaction_list(method, path, params):
    """
    Get all transactions through pagination (listTransactions API)
    
    Args:
        method: HTTP method
        path: API endpoint path
        params: Query parameters
    
    Returns:
        list: All transactions
    """
    all_transactions = []
    
    # Initial API call
    json_response = spapi_request(method=method, path=path, params=params)
    
    if "errors" in json_response:
        return all_transactions
    
    payload = json_response.get("payload")
    if not payload:
        return all_transactions
    
    # Extract transactions from payload
    transactions = payload.get("transactions", [])
    all_transactions.extend(transactions)
    
    # Paginate through remaining results
    next_token = payload.get("nextToken")
    
    while next_token:
        json_response = spapi_request(
            method=method, 
            path=path, 
            params={"nextToken": next_token}
        )
        
        if "errors" in json_response:
            break
        
        payload = json_response.get("payload")
        if not payload:
            break
        
        transactions = payload.get("transactions", [])
        all_transactions.extend(transactions)
        
        next_token = payload.get("nextToken")
    
    return all_transactions

def extract_breakdown_value(breakdowns, breakdown_type, sub_type=None):
    """
    Extract specific breakdown amount from nested breakdowns structure
    
    Args:
        breakdowns: List of breakdown objects
        breakdown_type: Type to search for (e.g., "Commission", "FBAPerUnitFulfillmentFee")
        sub_type: Optional sub-type to search for (e.g., "Base", "Tax")
    
    Returns:
        float: The breakdown amount, or 0 if not found
    """
    if not breakdowns:
        return 0
    
    for breakdown in breakdowns:
        if breakdown.get("breakdownType") == breakdown_type:
            # If we need a sub-type (like "Base" or "Tax")
            if sub_type:
                nested = breakdown.get("breakdowns", [])
                for nested_breakdown in nested:
                    if nested_breakdown.get("breakdownType") == sub_type:
                        amount = nested_breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                        return abs(amount) if amount else 0
            else:
                # Return the main breakdown amount
                amount = breakdown.get("breakdownAmount", {}).get("currencyAmount", 0)
                return abs(amount) if amount else 0
        
        # Recursively search nested breakdowns
        nested = breakdown.get("breakdowns")
        if nested:
            result = extract_breakdown_value(nested, breakdown_type, sub_type)
            if result != 0:
                return result
    
    return 0

def calculate_fees_from_transaction(item):
    """
    Calculate all fees for a transaction item (new API structure)
    
    Args:
        item: Transaction item data
    
    Returns:
        dict: Fee breakdown with base amounts and VAT separated
    """
    item_breakdowns = item.get("breakdowns", [])
    
    # Extract Commission (Referral Fee) - Base and Tax separately
    referral_fee_base = extract_breakdown_value(item_breakdowns, "Commission", "Base")
    referral_fee_tax = extract_breakdown_value(item_breakdowns, "Commission", "Tax")
    referral_fee_total = referral_fee_base + referral_fee_tax
    
    # Extract FBA fees - Base and Tax separately
    fba_fee_base = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Base")
    fba_fee_tax = extract_breakdown_value(item_breakdowns, "FBAPerUnitFulfillmentFee", "Tax")
    fba_fee_total = fba_fee_base + fba_fee_tax
    
    # Extract ShippingChargeback (if exists)
    shipping_charge_back_fees = extract_breakdown_value(item_breakdowns, "ShippingChargeback")
    
    # Total fees (all including VAT)
    total_fees = referral_fee_total + fba_fee_total + shipping_charge_back_fees
    
    return {
        "referral_fee_base": referral_fee_base,
        "referral_fee_tax": referral_fee_tax,
        "referral_fee_total": referral_fee_total,
        "fba_fee_base": fba_fee_base,
        "fba_fee_tax": fba_fee_tax,
        "fba_fee_total": fba_fee_total,
        "shipping_charge_back_fees": shipping_charge_back_fees,
        "total": total_fees
    }

def calculate_customer_charges_from_transaction(item):
    """
    Calculate customer charges and promotions (new API structure)
    
    Args:
        item: Transaction item data
    
    Returns:
        dict: Charge breakdown
    """
    item_breakdowns = item.get("breakdowns", [])
    
    # Extract OurPricePrincipal (item price)
    item_price = extract_breakdown_value(item_breakdowns, "OurPricePrincipal")
    
    # Extract Shipping charges
    shipping_charge = extract_breakdown_value(item_breakdowns, "Shipping")
    if shipping_charge == 0:
        shipping_charge = extract_breakdown_value(item_breakdowns, "ShippingPrincipal")
    
    # Extract promotions (PromoRebates or ShippingDiscount)
    total_promotions = extract_breakdown_value(item_breakdowns, "PromoRebates")
    if total_promotions == 0:
        total_promotions = extract_breakdown_value(item_breakdowns, "ShippingDiscount")
    
    # Total charges = item price + shipping
    total_charges = item_price + shipping_charge
    
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
    Process financial transactions and return transaction details
    
    Args:
        params: Query parameters for API call
        db_cursor: Database cursor
    
    Returns:
        list: List of transaction dictionaries
    """
    method = "GET"
    path = "/finances/2024-06-19/transactions"
    
    # Step 1: Retrieve all transactions from API
    api_transactions = retrieve_transaction_list(method, path, params)
    
    # Step 2: Extract all unique seller SKUs from contexts
    all_seller_skus = []
    for transaction in api_transactions:
        for item in transaction.get("items", []):
            for context in item.get("contexts", []):
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    if sku:
                        all_seller_skus.append(sku)
    
    all_seller_skus = list(set(all_seller_skus))
    
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
    
    for api_transaction in api_transactions:
        # Get transaction type and status
        transaction_type = api_transaction.get("transactionType", "Unknown")
        transaction_status = api_transaction.get("transactionStatus", "Unknown")
        
        # Get order ID from relatedIdentifiers
        order_id = None
        for identifier in api_transaction.get("relatedIdentifiers", []):
            if identifier.get("relatedIdentifierName") == "ORDER_ID":
                order_id = identifier.get("relatedIdentifierValue")
                break
        
        for item in api_transaction.get("items", []):
            # Extract SKU and ASIN from contexts
            sku = None
            asin = None
            for context in item.get("contexts", []):
                if context.get("contextType") == "ProductContext":
                    sku = context.get("sku")
                    asin = context.get("asin")
                    break
            
            if not sku:
                continue  # Skip if no SKU found
            
            # Initialize transaction record
            transaction = {}
            
            # Transaction metadata
            transaction["TransactionType"] = transaction_type
            transaction["TransactionStatus"] = transaction_status
            
            # Basic identifiers
            transaction["AmazonOrderId"] = order_id or "Not Available"
            transaction["SKU"] = sku
            
            # Get mapping for this SKU: SKU -> ASIN -> SSKU
            mapping = product_mapping.get(sku, {})
            
            transaction["ASIN"] = asin or mapping.get("asin", "Not Available")
            transaction["SSKU"] = mapping.get("ssku", "Not Available")
            
            # Get product details for this ASIN
            asin_key = transaction["ASIN"]
            details = asin_details.get(asin_key, {})
            
            transaction["Brand"] = details.get("brand", "Not Available")
            transaction["Category"] = details.get("category", "Not Available")
            
            # Calculate customer charges, promotions, and sales proceed
            charges = calculate_customer_charges_from_transaction(item)

            # Currency Code might differ
            transaction["Currency"] = item.get("totalAmount", {}).get("currencyCode", BASE_CURRENCY_CODE)
            
            # Item listing price and shipping
            transaction["SOLD"] = charges["item_price"]
            transaction["ShippingCharge"] = charges["shipping_charge"]
            
            # Promotions (negative value - shows discount given)
            transaction["TotalPromotions"] = -charges["total_promotions"]
            
            # Sales Proceed (what customer actually paid after promotions)
            transaction["SalesProceed"] = charges["sales_proceed"]
            
            # Calculate all fees (extracted from API breakdowns)
            fees = calculate_fees_from_transaction(item)

            # Referral Fee was asked to be named as Fee
            transaction["Fee"] = -fees["referral_fee_total"]
            transaction["FBAFees"] = -fees["fba_fee_total"]
            transaction["ShippingChargeback"] = -fees["shipping_charge_back_fees"]
            transaction["TotalAmazonFees"] = -fees["total"]
            
            # Calculate government VAT (% of item price, negative) - MANUAL from .env
            vat_amount = charges["item_price"] * GOVT_VAT_RATE
            transaction["VAT"] = -vat_amount
            
            # R.VAT - VAT on fees (extracted from API, can be claimed back)
            # This is the Tax component from Commission + FBAPerUnitFulfillmentFee
            fees_vat = fees["referral_fee_tax"] + fees["fba_fee_tax"]
            transaction["R.VAT"] = fees_vat

            # Referral Fee % without VAT
            # Use the base referral fee (without VAT) to calculate percentage
            if charges["item_price"] != 0:
                transaction["Fee%"] = (fees["referral_fee_base"] / charges["item_price"]) * 100
            else:
                transaction["Fee%"] = 0

            # Get and parse cost
            cost = parse_cost(details.get("cost"))
            
            # Store cost of goods
            if cost is None:
                transaction["COG"] = "Not Available"
            else:
                transaction["COG"] = -cost
            
            # Calculate net profit
            net_profit = calculate_profit(
                currency_code=transaction["Currency"],
                sales_proceed=charges["sales_proceed"],
                fees_total=fees["total"],
                vat_amount=vat_amount,
                fees_vat=fees_vat,
                cost=cost
            )
            
            transaction["Net Profit"] = net_profit
            
            transactions.append(transaction)
    
    return transactions