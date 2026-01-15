# auth.py
import requests
import os
from dotenv import load_dotenv
from datetime import datetime, timedelta

load_dotenv()


LWA_TOKEN_URL = os.getenv("LWA_TOKEN_URL")


CLIENT_ID = os.getenv("LWA_CLIENT_ID")
CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN")
# This should come from .env
SPAPI_ENDPOINT = os.getenv("SPAPI_ENDPOINT")


# Implemented caching here, to make sure lwa access 
# token is not being requested on every spapi_request
# we take advantage of the fact that LWA tokens last 3600 seconds
_cached_token = None
_cached_token_expiry = None

def get_lwa_access_token():
   """
   Exchanges the refresh token for a short-lived LWA access token.
   No AWS IAM or SigV4 required.
   """
   global _cached_token, _cached_token_expiry

   if(_cached_token and (datetime.utcnow() < _cached_token_expiry)):
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
   # reset the expiry (1 min before to be safer). get expiry time from data itself otherwise default to 3600s
   _cached_token_expiry = datetime.utcnow() + timedelta(data.get("expires_in", 3600) - 60)

   return _cached_token




def spapi_request(method, path, params=None):
   """
   Makes a direct SP-API call using only the LWA access token.
   No AWS signing required.
   """
   access_token = get_lwa_access_token()

   url = SPAPI_ENDPOINT + path


   headers = {
       "x-amz-access-token": access_token,
       "Content-Type": "application/json",
   }


   response = requests.request(method, url, headers=headers, params=params)


   try:
       return response.json()
   except:
       return {"error": "Invalid JSON response", "raw": response.text}
  