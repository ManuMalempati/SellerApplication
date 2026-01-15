# auth.py
import requests
import os
from dotenv import load_dotenv


load_dotenv()


LWA_TOKEN_URL = os.getenv("LWA_TOKEN_URL")


CLIENT_ID = os.getenv("LWA_CLIENT_ID")
CLIENT_SECRET = os.getenv("LWA_CLIENT_SECRET")
REFRESH_TOKEN = os.getenv("LWA_REFRESH_TOKEN")


# This should come from .env
SPAPI_ENDPOINT = os.getenv("SPAPI_ENDPOINT")


def get_lwa_access_token():
   """
   Exchanges the refresh token for a short-lived LWA access token.
   No AWS IAM or SigV4 required.
   """
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


   return data["access_token"]




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
  