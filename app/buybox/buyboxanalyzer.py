# RESPONSIBLE FOR FBABuyBoxAnalysis Table
import time
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
# BuyBox Analyzer
# ---------------------------------------------------------
class BuyBoxAnalysis:

    def run(self):
        asin_data = self.get_asins()
        total = len(asin_data)

        if total == 0:
            print("No ASINs to process")
            return

        print(f"Starting analysis for {total} ASINs...")

        # ---------------------------------------------------------
        # 1. Open ONE DB connection for entire run
        # ---------------------------------------------------------
        conn = connect_database()
        cursor = conn.cursor()
        cursor.fast_executemany = True

        # ---------------------------------------------------------
        # 2. Pre-delete all ASINs in one go
        # ---------------------------------------------------------
        asin_list = [item["asin"] for item in asin_data]
        placeholders = ",".join("?" for _ in asin_list)

        cursor.execute(
            f"DELETE FROM spapi_app_user.FBABuyBoxAnalysis WHERE asin IN ({placeholders})",
            asin_list
        )
        conn.commit()

        # ---------------------------------------------------------
        # 3. Process all ASINs and collect results
        # ---------------------------------------------------------
        results = []
        step = max(1, total // 10)

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

            time.sleep(0.2)

        # ---------------------------------------------------------
        # 4. Bulk insert all results at once
        # ---------------------------------------------------------
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
        print("Analysis successfully completed.")

if __name__ == "__main__":
    BuyBoxAnalysis().run()