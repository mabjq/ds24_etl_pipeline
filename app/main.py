# app/main.py
# Main script to run the ETL pipeline: fetch prices, transform, load to database, and visualize.

import logging
from app.logger import setup_logger
from app.api import fetch_metal_prices
from app.transform import transform_prices
from app.database import create_connection, get_latest_price, insert_price
from app.visualize import create_price_plot
from config.config import ERROR_LOG

# Setup logger
logger = logging.getLogger("gold_silver_etl.main")
logger.addHandler(setup_logger(logfile=ERROR_LOG))
logger.setLevel(logging.INFO)

def run_etl():
    """
    Runs the ETL pipeline: fetches prices, transforms, loads to database, and creates plot.
    """
    logger.info("Starting ETL pipeline.")

    # Step 1: Extract
    prices = fetch_metal_prices()
    if not prices:
        logger.error("ETL stopped: failed to fetch prices.")
        return

    # Step 2: Connect to database
    conn = create_connection()
    if not conn:
        logger.error("ETL stopped: failed to connect to database.")
        return

    try:
        # Step 3: Get previous prices for change calculation
        previous_gold = get_latest_price(conn, "gold")
        previous_silver = get_latest_price(conn, "silver")
        previous_gold_price = previous_gold[0] if previous_gold else None
        previous_silver_price = previous_silver[0] if previous_silver else None

        # Step 4: Transform
        transformed = transform_prices(prices, previous_gold_price, previous_silver_price)
        if not transformed:
            logger.error("ETL stopped: transformation failed.")
            return
        gold_data, silver_data = transformed

        # Step 5: Load
        inserted_gold = insert_price(
            conn, 
            gold_data["date"], 
            gold_data["metal"], 
            gold_data["price_usd"], 
            gold_data["price_change"]
        )
        inserted_silver = insert_price(
            conn, 
            silver_data["date"], 
            silver_data["metal"], 
            silver_data["price_usd"], 
            silver_data["price_change"]
        )

        if inserted_gold or inserted_silver:
            logger.info("New data inserted. Creating price plot.")
            create_price_plot()
        else:
            logger.info("No new data inserted. Skipping plot creation.")

    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
    finally:
        conn.close()

if __name__ == "__main__":
    run_etl()