# app/api.py
import requests
import logging
from datetime import datetime
from config.config import COINGECKO_API_KEY
from app.logger import setup_logger

# Setup logger
logger = logging.getLogger("gold_silver_etl")
logger.addHandler(setup_logger(logfile="logs/errors.log"))

def fetch_metal_prices() -> dict | None:
    """
    Fetches current prices for gold and silver from the CoinGecko API.

    Returns:
        dict: A dictionary with prices (e.g., {"pax-gold": 2500.00, "silver-token-xagx": 30.00, "timestamp": "2025-08-28T17:03:00"})
              or None if the call fails.
    """
    url = "https://api.coingecko.com/api/v3/simple/price"
    params = {
        "ids": "pax-gold,silver-token-xagx",
        "vs_currencies": "usd",
        "x_cg_demo_api_key": COINGECKO_API_KEY
    }

    try:
        response = requests.get(url, params=params, timeout=10)
        if response.status_code != 200:
            logger.error(f"API call failed with status code {response.status_code}")
            return None

        data = response.json()
        if "pax-gold" not in data or "silver-token-xagx" not in data:
            logger.error("API response is missing data for pax-gold or silver-token-xagx")
            return None
        if "usd" not in data["pax-gold"] or "usd" not in data["silver-token-xagx"]:
            logger.error("API response is missing USD prices for pax-gold or silver-token-xagx")
            return None

        # Add a timestamp for when the data was fetched
        result = {
            "pax-gold": data["pax-gold"]["usd"],
            "silver-token-xagx": data["silver-token-xagx"]["usd"],
            "timestamp": datetime.now().isoformat()
        }
        logger.info("Prices successfully fetched from CoinGecko")
        return result

    except requests.exceptions.RequestException as e:
        logger.error(f"Network error during API call: {str(e)}")
        return None
    except ValueError as e:
        logger.error(f"Error parsing API response: {str(e)}")
        return None