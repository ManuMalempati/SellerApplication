# SellerAPIApplication
A FastAPI backend that integrates with Amazon’s Selling Partner API (SP‑API) to retrieve Financial Events, calculate profit per order, and enrich transactions using your internal SKU → SSKU cost mapping stored in SQL Server.

This project is designed for Amazon sellers who want accurate, real‑time profitability analytics without relying on third‑party tools.

Features
SP‑API Integration
Connects to Amazon SP‑API using Login With Amazon (LWA) tokens

Fetches Financial Events (ShipmentEventList)

Handles pagination via NextToken

Supports UAE marketplace (EU endpoint)

Profit Engine
For each shipped item, the system calculates:

Item listing price

Referral fees

FBA fees

Government VAT

Product buying cost (via SQL Server lookup)

Net profit per item

Database Integration
Connects to SQL Server

Retrieves product cost using SKU → SSKU mapping

Supports fallback logic for missing SKUs

FastAPI Endpoints
/financial-events → Returns processed transactions with profit

/raw-financial-events → Returns raw SP‑API response for debugging


Environment Variables
Create a .env file in the project root:

Code
LWA_TOKEN_URL=...
LWA_CLIENT_ID=...
LWA_CLIENT_SECRET=...
LWA_REFRESH_TOKEN=...
SPAPI_ENDPOINT=https://sellingpartnerapi-eu.amazon.com
GOVT_VAT_RATE=0.05
DB_SERVER=...
DB_USER=...
DB_PASSWORD=...
DB_NAME=...


Installation
1. Clone the repository
git clone https://github.com/ManuMalempati/SellerAPIApplication.git
cd SellerAPIApplication
2. Create a virtual environment
python3 -m venv venv
source venv/bin/activate
3. Install dependencies
pip install -r requirements.txt

Running the Server
Start FastAPI with Uvicorn:
uvicorn main:app --reload
The API will be available at:
http://127.0.0.1:5002

API Endpoints
GET /financial-events
Returns processed transactions with:
- Fees
- VAT
- Product cost
- Net profit

GET /raw-financial-events
Returns the raw SP‑API response for debugging.

How Profit Is Calculated
Code
Net Profit =
    ItemListingPrice
  + ReferralFee
  + FBAFees
  + GovernmentVAT
  + ProductBuyingPrice (negative)
All Amazon fees are already negative in SP‑API.
