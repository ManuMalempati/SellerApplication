# auth.py
import requests
import os
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

LWA_TOKEN_URL = os.getenv("LWA_TOKEN_URL")
CLIENT_ID = os.getenv("LWA_CLIENT_ID")
CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN")
SPAPI_ENDPOINT = os.getenv("SPAPI_ENDPOINT")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

_cached_token = None
_cached_token_expiry = None

def get_lwa_access_token():
   global _cached_token, _cached_token_expiry
   if(_cached_token and (datetime.now(timezone.utc) < _cached_token_expiry)):
       return _cached_token

   response = requests.post(
       LWA_TOKEN_URL,
       data={
           "grant_type": "refresh_token",
           "refresh_token": REFRESH_TOKEN,
           "client_id": CLIENT_ID,
           "client_secret": CLIENT_SECRET,
       },
   )
   data = response.json()
   if "access_token" not in data:
       raise Exception(f"LWA token error: {data}")
   
   _cached_token = data["access_token"]
   _cached_token_expiry = datetime.now(timezone.utc) + timedelta(seconds=data.get("expires_in", 3600) - 60)
   return _cached_token

def spapi_request(method, path, params=None, body=None):
   access_token = get_lwa_access_token()
   url = SPAPI_ENDPOINT + path
   headers = {
       "x-amz-access-token": access_token,
       "Content-Type": "application/json",
   }

   if params is None: params = {}

   # Logic fix: Only add MarketplaceIds if MarketplaceId isn't already there
   if method.upper() == "GET":
       if "MarketplaceId" not in params and "MarketplaceIds" not in params:
           params["MarketplaceIds"] = MARKETPLACE_ID

   response = requests.request(method, url, headers=headers, params=params, json=body)
   try:
       return response.json()
   except:
       return {"error": "Invalid JSON response", "raw": response.text}