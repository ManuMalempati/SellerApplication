import json
from .database import get_all_product_costs, get_asins_from_db_or_api
from .auth import spapi_request
import os
import time
import math

GOVT_VAT_RATE = float(os.getenv("GOVT_VAT_RATE"))

def get_asins_batch(seller_sku_list):
    """Get ASINs for multiple SKUs"""
    sku_to_asin = {}
    
    for sku in seller_sku_list:
        path = "/catalog/2022-04-01/items"
        params = {
            "identifiers": sku,
            "identifiersType": "SKU",
            "includedData": "identifiers"
        }
        
        response = spapi_request(method="GET", path=path, params=params)
        
        if response.get("items") and len(response["items"]) > 0:
            print(response["items"][0].get("asin"))
            sku_to_asin[sku] = response["items"][0].get("asin")
    
    return sku_to_asin


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

   # We don't know all ASINS for each SKU yet.
   all_seller_skus = list(set([item["SellerSKU"] for order in shipmentEventList for item in (order.get("ShipmentItemList") or [])]))
   
   # get all ASINS
   sku_to_asin = get_asins_from_db_or_api(db_cursor, all_seller_skus)
   all_asins = list(sku_to_asin.values())

   asin_to_cost, asin_to_ssku = get_all_product_costs(db_cursor, all_asins)

   transactions = []

   for order in shipmentEventList:
       for item in order["ShipmentItemList"]:
           transaction = {}

           # Basic fields
           transaction["AmazonOrderId"] = order["AmazonOrderId"]
           transaction["SKU"] = item["SellerSKU"]
           transaction["ASIN"] = sku_to_asin.get(transaction["SKU"], "Not Available")

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

           # remember we got this from SKU using API call
           asin = transaction["ASIN"]
           cost_str = asin_to_cost.get(asin)
        
           # Parse cost
           cost = None
           if cost_str:
               try:
                   cost = float(cost_str.replace("$", "").replace(",", "").strip())
               except ValueError:
                   pass

           # Product cost
           transaction["ProductBuyingPrice"] = cost
           transaction["SSKU"] = asin_to_ssku.get(asin, "Not Available")

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