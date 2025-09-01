# app/visualize.py
# Module for visualizing gold and silver prices with matplotlib, including extreme movements.

import sqlite3
import logging
import matplotlib.pyplot as plt
from datetime import datetime
from typing import List, Tuple
from app.logger import setup_logger
from app.database import create_connection
from config.config import ERROR_LOG, EXTREME_THRESHOLD

# Setup logger
logger = logging.getLogger("gold_silver_etl.visualize")
logger.addHandler(setup_logger(logfile=ERROR_LOG))

def fetch_prices_for_plotting(conn: sqlite3.Connection) -> Tuple[List[Tuple[str, float, float]], List[Tuple[str, float, float]]]:
    """Fetches all price data from the database for gold and silver.
    
    Args:
        conn (sqlite3.Connection): Database connection.
    
    Returns:
        Tuple[List[Tuple[str, float, float]], List[Tuple[str, float, float]]]: Lists of (date, price_usd, 
        price_change) for gold and silver.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("SELECT date, price_usd, price_change FROM PreciousMetals WHERE metal = 'gold' ORDER BY date")
        gold_data = cursor.fetchall()
        cursor.execute("SELECT date, price_usd, price_change FROM PreciousMetals WHERE metal = 'silver' ORDER BY date")
        silver_data = cursor.fetchall()
        logger.info("Fetched price data for plotting.")
        return gold_data, silver_data
    except sqlite3.Error as e:
        logger.error(f"Database error fetching prices for plotting: {str(e)}")
        return [], []

def create_price_plot() -> bool: 
    """ 
    Creates a line plot of gold and silver prices over time, marking extreme movements. 
 
    Returns: 
        bool: True if plot saved successfully, False otherwise. 
    """ 
    conn = create_connection() 
    if not conn: 
        logger.error("Cannot create plot: database connection failed.") 
        return False 
 
    try: 
        gold_data, silver_data = fetch_prices_for_plotting(conn) 
         
        if not gold_data and not silver_data: 
            logger.error("No data available for plotting.") 
            return False 
 
        # Prepare data for plotting 
        gold_dates = [datetime.fromisoformat(row[0]) for row in gold_data] 
        gold_prices = [row[1] for row in gold_data] 
        gold_changes = [row[2] for row in gold_data] 
         
        silver_dates = [datetime.fromisoformat(row[0]) for row in silver_data] 
        silver_prices = [row[1] for row in silver_data] 
        silver_changes = [row[2] for row in silver_data] 
 
        # Create plot with dual y-axes 
        fig, ax1 = plt.subplots(figsize=(10, 6)) 
 
        # Gold on left y-axis 
        ax1.plot(gold_dates, gold_prices, label="Gold (USD/oz)", color="gold", marker=".", markersize=4, linewidth=1) 
        ax1.set_xlabel("Date") 
        ax1.set_ylabel("Gold Price (USD/oz)", color="gold") 
        ax1.tick_params(axis='y', labelcolor="gold") 
 
        # Silver on right y-axis 
        ax2 = ax1.twinx() 
        ax2.plot(silver_dates, silver_prices, label="Silver (USD/oz)", color="silver", marker=".", markersize=4, linewidth=1) 
        ax2.set_ylabel("Silver Price (USD/oz)", color="silver") 
        ax2.tick_params(axis='y', labelcolor="silver") 
 
        # Mark extreme movements with red stars (smaller size for consistency) 
        for date, price, change in gold_data: 
            if abs(change) > EXTREME_THRESHOLD: 
                ax1.scatter([datetime.fromisoformat(date)], [price], color="red", s=50, marker="*") 
 
        for date, price, change in silver_data: 
            if abs(change) > EXTREME_THRESHOLD: 
                ax2.scatter([datetime.fromisoformat(date)], [price], color="red", s=50, marker="*") 
 
        # Set x-axis to 2025 only 
        ax1.set_xlim(datetime(2025, 1, 1), datetime(2025, 12, 31)) 
 
        plt.title("Gold and Silver Prices in 2025") 
        handles1, labels1 = ax1.get_legend_handles_labels() 
        handles2, labels2 = ax2.get_legend_handles_labels() 
        ax1.legend(handles1 + handles2, labels1 + labels2, loc='upper left') 
        plt.grid(True) 
        plt.xticks(rotation=45) 
        plt.tight_layout() 
 
        # Save plot 
        plt.savefig("results/prices.png") 
        plt.close() 
        logger.info("Price plot saved to results/prices.png.") 
        return True 
    except Exception as e: 
        logger.error(f"Error creating price plot: {str(e)}") 
        return False 
    finally: 
        conn.close()