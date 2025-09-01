# Module for handling SQLite database operations for precious metals prices.

import sqlite3
import logging
from datetime import datetime
from app.logger import setup_logger
from config.config import DATABASE_PATH, ERROR_LOG

# Setup logger
logger = logging.getLogger("gold_silver_etl.database")
logger.addHandler(setup_logger(logfile=ERROR_LOG))

def create_connection() -> sqlite3.Connection | None:
    """
    Creates a connection to the SQLite database and ensures the table exists.

    Returns:
        sqlite3.Connection: Database connection or None on error.
    """
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS PreciousMetals (
                date TEXT NOT NULL,
                metal TEXT NOT NULL,
                price_usd REAL NOT NULL,
                price_change REAL,
                UNIQUE(date, metal)
            )
        """)
        conn.commit()
        logger.info("Database connection created and table verified.")
        return conn
    except sqlite3.Error as e:
        logger.error(f"Database error during connection: {str(e)}")
        return None

def get_latest_price(conn: sqlite3.Connection, metal: str) -> tuple[float, str] | None:
    """
    Fetches the latest price and date for a given metal from the database.

    Args:
        conn (sqlite3.Connection): Database connection.
        metal (str): Metal name (e.g., "gold" or "silver").

    Returns:
        tuple[float, str]: (latest price_usd, latest date) or None if no data or error.
    """
    try:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT price_usd, date FROM PreciousMetals
            WHERE metal = ? ORDER BY date DESC LIMIT 1
        """, (metal,))
        result = cursor.fetchone()
        if result:
            return result
        logger.info(f"No previous data for {metal}.")
        return None
    except sqlite3.Error as e:
        logger.error(f"Database error fetching latest price for {metal}: {str(e)}")
        return None

def insert_price(conn: sqlite3.Connection, date: str, metal: str, price_usd: float,
                price_change: float) -> bool:
    """
    Inserts new price data into the database if the date is newer than the latest entry.

    Args:
        conn (sqlite3.Connection): Database connection.
        date (str): ISO-format date.
        metal (str): Metal name.
        price_usd (float): Price in USD.
        price_change (float): Percentage change.

    Returns:
        bool: True if insertion succeeded, False otherwise.
    """
    try:
        # Validate inputs
        if not date or not metal or price_usd is None:
            logger.error(f"Invalid input for {metal}: date={sqlite3.sql_escape_string(str(date))}, price_usd={price_usd}")
            return False

        # Check latest date for the metal
        latest = get_latest_price(conn, metal)
        latest_date = latest[1] if latest else None
        if latest_date:
            try:
                if datetime.fromisoformat(date) <= datetime.fromisoformat(latest_date):
                    logger.info(f"Data for {metal} on {date} is not newer than {latest_date}. Skipping.")
                    return False
            except ValueError as e:
                logger.error(f"Invalid date format for {metal}: {date}. Error: {str(e)}")
                return False

        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO PreciousMetals (date, metal, price_usd, price_change)
            VALUES (?, ?, ?, ?)
        """, (date, metal, price_usd, price_change))
        conn.commit()
        logger.info(f"Price data inserted for {metal} on {date}.")
        return True
    except sqlite3.Error as e:
        logger.error(f"Database error inserting for {metal}: {str(e)}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error inserting for {metal}: {str(e)}")
        return False
