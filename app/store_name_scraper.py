# seller_name_lookup.py

import requests
from bs4 import BeautifulSoup
from typing import Optional

def get_seller_name(seller_id: str) -> Optional[str]:
    """
    Given a seller_id, fetch the seller storefront page and extract the
    <h1 id="seller-name">STORE NAME</h1> element.

    Returns:
        str  -> store name if found
        None -> if not found or error
    """

    try:
        url = f"https://www.amazon.ae/sp?seller={seller_id}"

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }

        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # Primary selector
        element = soup.select_one("#seller-name")

        # Fallback selector
        if not element:
            element = soup.select_one("h1")

        return element.get_text(strip=True) if element else None

    except Exception:
        return None
