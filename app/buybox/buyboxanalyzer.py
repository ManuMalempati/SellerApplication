# RESPONSIBLE FOR FBABuyBoxAnalysis Table
import time
import threading
from app.database import connect_database
from app.auth import spapi_request
from app.buybox.store_name_scraper import get_seller_name
from app.utilities.utils import (
    clean_str,
    safe_float,
    safe_int,
    now_utc_plus_offset_naive,
)
import config

MARKETPLACE_ID = config.MARKETPLACE_ID
SELLER_ID = config.SELLER_ID


# ---------------------------------------------------------
# Token Bucket Rate Limiter (0.5 RPS, burst 1)
# ---------------------------------------------------------

class TokenBucketRateLimiter:
    def __init__(self, rate: float, burst: int):
        self.rate = rate
        self.burst = burst
        self.tokens = burst
        self.last_update = time.time()
        self.lock = threading.Lock()

    def acquire(self):
        while True:
            with self.lock:
                now = time.time()
                elapsed = now - self.last_update
                self.tokens = min(self.burst, self.tokens + elapsed * self.rate)
                self.last_update = now

                if self.tokens >= 1.0:
                    self.tokens -= 1.0
                    return

                wait_time = (1.0 - self.tokens) / self.rate

            time.sleep(wait_time)


offers_rate_limiter = TokenBucketRateLimiter(rate=0.5, burst=1)


# ---------------------------------------------------------
# Models
# ---------------------------------------------------------

class OfferData:
    def __init__(self, offer):
        self.seller_id = clean_str(offer.get("SellerId"))
        self.listing_price = safe_float(offer.get("ListingPrice", {}).get("Amount"))
        self.shipping_cost = safe_float(offer.get("Shipping", {}).get("Amount"))
        self.is_buy_box_winner = bool(offer.get("IsBuyBoxWinner", False))
        self.is_fba = bool(offer.get("IsFulfilledByAmazon", False))
        self.is_prime = bool(offer.get("PrimeInformation", {}).get("IsPrime", False))

        rating = offer.get("SellerFeedbackRating", {})
        self.seller_rating = safe_float(rating.get("SellerPositiveFeedbackRating"))
        self.feedback_count = safe_int(rating.get("FeedbackCount"))

    @property
    def total_price(self):
        if self.listing_price is None:
            return None
        return self.listing_price + (self.shipping_cost or 0)


# ---------------------------------------------------------
# BuyBox Analyzer (Full ASIN Mode)
# ---------------------------------------------------------

class BuyBoxAnalysis:

    def get_asins(self):
        conn = connect_database()
        cursor = conn.cursor()

        cursor.execute("""
            SELECT asin, Title
            FROM spapi_app_user.FBAProductSummary
            WHERE asin IS NOT NULL AND [Sellable-Qty] > 0
        """)

        rows = cursor.fetchall()
        print(f"Total ASINs: {len(rows)}")

        cursor.close()
        conn.close()

        return [{"asin": r[0], "title": r[1]} for r in rows]

    def fetch_data(self, asin, title):
        offers_rate_limiter.acquire()

        try:
            response = spapi_request(
                method="GET",
                path=f"/products/pricing/v0/items/{asin}/offers",
                params={"MarketplaceId": MARKETPLACE_ID, "ItemCondition": "New"}
            )
            payload = response.get("payload", {})

            return {
                "asin": asin,
                "product_name": title,
                "summary": payload.get("Summary", {}),
                "offers": payload.get("Offers", []),
                "error": None
            }

        except Exception as e:
            print(f"Error fetching {asin}: {e}")
            return {
                "asin": asin,
                "product_name": title,
                "summary": {},
                "offers": [],
                "error": str(e)
            }

    def analyze(self, asin, product_name, summary, offers_raw):
        offers = [OfferData(o) for o in offers_raw]

        winner = next((o for o in offers if o.is_buy_box_winner), None)

        winner_store_name = (
            get_seller_name(winner.seller_id)
            if winner and winner.seller_id
            else None
        )

        my_offer = next((o for o in offers if o.seller_id == SELLER_ID), None)

        bb_prices = summary.get("BuyBoxPrices", [])
        summary_bb_price = safe_float(
            bb_prices[0]["LandedPrice"]["Amount"]
        ) if bb_prices else None

        lowest_amazon = None
        lowest_merchant = None

        for lp in summary.get("LowestPrices", []):
            if lp.get("fulfillmentChannel") == "Amazon":
                lowest_amazon = safe_float(lp["LandedPrice"]["Amount"])
            elif lp.get("fulfillmentChannel") == "Merchant":
                lowest_merchant = safe_float(lp["LandedPrice"]["Amount"])

        return {
            "asin": asin,
            "product_name": product_name,
            "winner_seller_id": winner.seller_id if winner else None,
            "winner_store_name": winner_store_name,
            "winner_price": winner.listing_price if winner else None,
            "winner_total_price": winner.total_price if winner else None,
            "my_price": my_offer.listing_price if my_offer else None,
            "my_shipping": my_offer.shipping_cost if my_offer else None,
            "my_total": my_offer.total_price if my_offer else None,
            "my_is_buybox": bool(my_offer.is_buy_box_winner) if my_offer else False,
            "summary_buybox_price": summary_bb_price,
            "lowest_price_amazon": lowest_amazon,
            "lowest_price_merchant": lowest_merchant,
            "analysis_timestamp": now_utc_plus_offset_naive(),
        }

    # ---------------------------------------------------------
    # MAIN RUN — FIXED SQL CONNECTION LIFECYCLE
    # ---------------------------------------------------------
    def run(self):
        asin_data = self.get_asins()
        total = len(asin_data)

        if total == 0:
            print("No ASINs to process")
            return

        print(f"Starting BuyBox analysis for {total} ASINs...")

        results = []
        step = max(1, total // 10)

        # ---------------------------------------------------------
        # 1. PROCESS ALL ASINS FIRST (NO SQL CONNECTION OPEN)
        # ---------------------------------------------------------
        for i, item in enumerate(asin_data, 1):
            if i == 1 or i % step == 0 or i == total:
                pct = int((i / total) * 100)
                print(f"[{pct}%] {i}/{total} : {item['asin']}")

            data = self.fetch_data(item["asin"], item["title"])
            result = self.analyze(
                data["asin"], data["product_name"],
                data["summary"], data["offers"]
            )

            results.append((
                result["asin"],
                result["product_name"],
                result["winner_seller_id"],
                result["winner_store_name"],
                result["winner_price"],
                result["winner_total_price"],
                result["my_price"],
                result["my_shipping"],
                result["my_total"],
                result["my_is_buybox"],
                result["summary_buybox_price"],
                result["lowest_price_amazon"],
                result["lowest_price_merchant"],
                result["analysis_timestamp"],
                now_utc_plus_offset_naive(),
                now_utc_plus_offset_naive()
            ))

        # ---------------------------------------------------------
        # 2. NOW OPEN SQL CONNECTION (FRESH + SAFE)
        # ---------------------------------------------------------
        conn = connect_database()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # 3. DELETE TABLE
        cursor.execute("DELETE FROM spapi_app_user.FBABuyBoxAnalysis")
        conn.commit()

        # 4. BULK INSERT
        cursor.executemany("""
            INSERT INTO spapi_app_user.FBABuyBoxAnalysis (
                asin, product_name, winner_seller_id, winner_store_name,
                winner_price, winner_total_price, my_price, my_shipping,
                my_total, my_is_buybox, summary_buybox_price, lowest_price_amazon,
                lowest_price_merchant, analysis_timestamp, created_at, updated_at
            )
            VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
        """, results)

        conn.commit()
        cursor.close()
        conn.close()

        print("-" * 30)
        print("BuyBox analysis completed successfully.")


if __name__ == "__main__":
    BuyBoxAnalysis().run()
