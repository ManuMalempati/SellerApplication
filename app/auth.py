"""Lightweight helper for LWA token management and SP-API requests.

Provides a cached LWA access token (with a small safety margin before expiry)
and a helper to call SP-API endpoints, ensuring MarketplaceIds are present for GETs.
"""
import requests
import os
from urllib.parse import quote
from dotenv import load_dotenv
from datetime import datetime, timedelta, timezone

load_dotenv()

LWA_TOKEN_URL = os.getenv("LWA_TOKEN_URL", "https://api.amazon.com/auth/o2/token")
CLIENT_ID = os.getenv("LWA_CLIENT_ID")
CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN")
SPAPI_ENDPOINT = os.getenv("SPAPI_ENDPOINT")
MARKETPLACE_ID = os.getenv("MARKETPLACE_ID")

# Cached token and its expiry (timezone-aware)
_cached_token = None
_cached_token_expiry = None

# SP-API Does not require AWS/SigV4 anymore
def get_lwa_access_token():
   global _cached_token, _cached_token_expiry
   # Return cached token if it exists and hasn't expired yet
   if(_cached_token and (datetime.now(timezone.utc) < _cached_token_expiry)):
       return _cached_token

   # Request a new token using the refresh token flow
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
       # Provide a clear exception if token acquisition failed
       raise Exception(f"LWA token error: {data}")
   
   # Cache the token and set expiry slightly before the real expiry to be safe
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

   # Ensure MarketplaceIds is added for GET requests unless already present
   if method.upper() == "GET":
       if "MarketplaceId" not in params and "MarketplaceIds" not in params:
           params["MarketplaceIds"] = MARKETPLACE_ID

   # Execute the HTTP request and attempt to parse JSON; fall back to raw text on failure
   response = requests.request(method, url, headers=headers, params=params, json=body)
   try:
       return response.json()
   except:
       return {"error": "Invalid JSON response", "raw": response.text}