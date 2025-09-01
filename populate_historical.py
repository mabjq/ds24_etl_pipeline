import datetime
import time
import requests
import logging
from config.config import COINGECKO_API_KEY, EXTREME_THRESHOLD, ERROR_LOG, EXTREME_LOG 
from app.logger import setup_logger
from app.database import create_connection, get_latest_price, insert_price
from app.transform import calculate_change, flag_extreme_movement 

# Setup logger
logger = logging.getLogger("gold_silver_etl.populate_historical")
logger.addHandler(setup_logger(logfile=ERROR_LOG))
logger.setLevel(logging.INFO)

def fetch_historical_price(coin_id: str, date_str: str) -> float | None:
    """Fetch historical USD price for a given coin and date from CoinGecko."""
    url = f"https://api.coingecko.com/api/v3/coins/{coin_id}/history"
    params = {
        "date": datetime.datetime.strptime(date_str, '%Y-%m-%d').strftime('%d-%m-%Y'),  
        "localization": "false",
        "x_cg_demo_api_key": COINGECKO_API_KEY
    }
    try:
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()
        price_usd = data.get('market_data', {}).get('current_price', {}).get('usd')
        if price_usd is None:
            logger.error(f"No USD price found for {coin_id} on {date_str}")
            return None
        return float(price_usd)
    except requests.exceptions.RequestException as e:
        logger.error(f"Failed to fetch historical price for {coin_id} on {date_str}: {e}")
        return None

# Start and end dates for 2025 so far
start_date = datetime.date(2025, 1, 1)
end_date = datetime.date(2025, 8, 31)  
delta = datetime.timedelta(days=1)

conn = create_connection()
if not conn:
    logger.error("Failed to connect to database. Aborting.")
    exit(1)

try:
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.isoformat() + "T00:00:00"  
        date_only = current_date.strftime('%Y-%m-%d')

        for metal, coin_id in [('gold', 'pax-gold'), ('silver', 'silver-token-xagx')]:
            price_usd = fetch_historical_price(coin_id, date_only)
            if price_usd is None:
                continue

            # Get previous price from DB 
            prev = get_latest_price(conn, metal)
            prev_price = prev[0] if prev and datetime.datetime.fromisoformat(prev[1]) < datetime.datetime.fromisoformat(date_str) else None

            price_change = calculate_change(prev_price, price_usd) if prev_price else 0.0
            flag_extreme_movement(metal, date_str, price_change)

            # Insert
            inserted = insert_price(conn, date_str, metal, price_usd, price_change)
            if inserted:
                logger.info(f"Inserted historical data for {metal} on {date_str}")

        time.sleep(10)  # Wait 10 sec for rate limit
        current_date += delta
finally:
    conn.close()

print("Historical data populated successfully.")
