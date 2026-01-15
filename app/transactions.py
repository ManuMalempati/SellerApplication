import json
from .database import get_product_cost_with_sku, get_all_product_costs
from .auth import spapi_request
import os
import time
import math

GOVT_VAT_RATE = float(os.getenv("GOVT_VAT_RATE"))


# Get all the shipment events through pagination
def retrieve_shipment_list(method, path, params):
   all_data = []

   json_response = spapi_request(method=method, path=path, params=params)

   if "errors" in json_response: 
       return all_data

   payload = json_response.get("payload")

   if not payload:
       return all_data
  
   # Helper function to add events to the existing all_data list
   def add_shipment_events(payload):
       events = payload.get("FinancialEvents", {})
       shipment_list = events.get("ShipmentEventList", [])
       all_data.extend(shipment_list)

   add_shipment_events(payload)

   next_token = json_response["payload"].get("NextToken")

   # Paginate until next_token is not provided in the payload
   while next_token:
       # Only provide NextToken instead of PostedAfter
       json_response = spapi_request(method=method, path=path, params={"NextToken": next_token})

       if "errors" in json_response: 
           break

       payload = json_response.get("payload")
       if not payload:
           break

       add_shipment_events(payload)
      
       next_token = payload.get("NextToken")
  
   return all_data


def get_transactions(params, db_cursor):
   
   method="GET"
   path="/finances/v0/financialEvents"

   shipmentEventList = retrieve_shipment_list(method, path, params)

   all_skus = [item["SellerSKU"] for order in shipmentEventList for item in (order.get("ShipmentItemList") or [])]

   cost_cache, stripped_map = get_all_product_costs(db_cursor, all_skus)

   transactions = []

   for order in shipmentEventList:
       for item in order["ShipmentItemList"]:
           transaction = {}

           # Basic fields
           transaction["AmazonOrderId"] = order["AmazonOrderId"]
           transaction["SKU"] = item["SellerSKU"]


           # Item price (Principal)
           item_price = 0
           for charge in item["ItemChargeList"]:
               if charge["ChargeType"] == "Principal":
                   item_price += charge["ChargeAmount"]["CurrencyAmount"]


           transaction["ItemListingPrice"] = item_price


           # Separate fees
           referral_fee = 0
           fba_fees = 0
           other_fees = 0

           fees = item.get("ItemFeeList") or []
           for fee in fees:
               fee_type = fee["FeeType"]
               amount = fee["FeeAmount"]["CurrencyAmount"]


               # In UAE Marketplace, it is named as Commission
               if fee_type in ("ReferralFee", "Commission"):
                   referral_fee += amount
               # Again, FBA fee names vary so we do this
               elif fee_type.startswith("FBA"):
                   fba_fees += amount
               else:
                   other_fees += amount
          
           # Government VAT (fixed 5%)
           vat_amount = item_price * GOVT_VAT_RATE * -1


           # Store fees, remember all fees here are in negative values
           transaction["ReferralFee"] = referral_fee
           transaction["FBAFees"] = fba_fees
           transaction["GovernmentVAT"] = vat_amount
           transaction["TotalAmazonFees"] = referral_fee + fba_fees + other_fees

           sku = transaction["SKU"]
           cost_str = cost_cache.get(sku) or cost_cache.get(stripped_map.get(sku, sku))
            
           # Parse cost
           cost = None
           if cost_str:
               try:
                   cost = float(cost_str.replace("$", "").replace(",", "").strip())
               except ValueError:
                   pass

           # Product cost
           transaction["ProductBuyingPrice"] = cost
           transaction["SSKU Used"] = stripped_map.get(sku, sku)

           # Profit formula
           # since fees are in negative already
           transaction["Net Profit"] = (
               item_price
               + (referral_fee + fba_fees + vat_amount)
           )

           if(transaction["ProductBuyingPrice"] == None):
               transaction["ProductBuyingPrice"] = "Not Available"
               transaction["Net Profit"] = "Not Available"
           else:
               # Make fee negative to show that it is money out
               transaction["ProductBuyingPrice"] *= -1
               transaction["Net Profit"] = round(transaction["Net Profit"] + transaction["ProductBuyingPrice"], 2)
          
           transactions.append(transaction)
   return transactions