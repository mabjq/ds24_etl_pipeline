# Module for transforming fetched price data: currency conversion, change calculation, validation, and extreme movement flagging.

import logging
from datetime import datetime
from typing import Dict, Optional, Tuple
from app.logger import setup_logger
from config.config import EXTREME_THRESHOLD, ERROR_LOG, EXTREME_LOG

# Setup loggers: one for errors, one for extreme movements
error_logger = logging.getLogger("gold_silver_etl.transform.errors")
error_logger.addHandler(setup_logger(logfile=ERROR_LOG))

extreme_logger = logging.getLogger("gold_silver_etl.transform.extreme")
extreme_logger.addHandler(setup_logger(logfile=EXTREME_LOG))
extreme_logger.setLevel(logging.INFO)  # Log extreme events at INFO level

def validate_data(prices: Dict[str, any]) -> bool:
    """
    Validates the fetched prices dictionary for required fields and types.

    Args:
        prices (Dict[str, any]): The dictionary from API fetch.

    Returns:
        bool: True if valid, False otherwise (logs errors).
    """
    required_keys = ["pax-gold", "silver-token-xagx", "timestamp"]
    if not all(key in prices for key in required_keys):
        error_logger.error("Missing required keys in prices data.")
        return False
    
    try:
        float(prices["pax-gold"])
        float(prices["silver-token-xagx"])
        datetime.fromisoformat(prices["timestamp"])
    except (ValueError, TypeError) as e:
        error_logger.error(f"Invalid data types in prices: {str(e)}")
        return False
    
    return True

def calculate_change(previous_usd: float, current_usd: float) -> float:
    """
    Calculates percentage change from previous to current price.

    Args:
        previous_usd (float): Previous USD price.
        current_usd (float): Current USD price.

    Returns:
        float: Percentage change, or 0.0 if division by zero or invalid.
    """
    if previous_usd == 0:
        error_logger.warning("Previous price is zero; cannot calculate change.")
        return 0.0
    try:
        change = ((current_usd - previous_usd) / previous_usd) * 100
        return round(change, 2)  
    except (TypeError, ValueError) as e:
        error_logger.error(f"Error calculating change: {str(e)}")
        return 0.0

def flag_extreme_movement(metal: str, timestamp: str, change: float) -> None:
    """
    Flags and logs extreme price movements if above threshold.

    Args:
        metal (str): Metal name (e.g., "gold").
        timestamp (str): ISO timestamp.
        change (float): Percentage change.
    """
    if abs(change) > EXTREME_THRESHOLD:
        direction = "+" if change > 0 else "-"
        extreme_logger.info(f"{timestamp} - Extreme price movement for {metal}: {direction}{abs(change)}%")

def transform_prices(prices: Dict[str, any], previous_gold: Optional[float] = None, 
                     previous_silver: Optional[float] = None) -> Optional[Tuple[Dict[str, any], Dict[str, any]]]:
    """
    Transforms the fetched prices: validates, calculates changes, flags extremes.

    Args:
        prices (Dict[str, any]): Raw prices from API.
        previous_gold (Optional[float]): Previous gold USD price from DB.
        previous_silver (Optional[float]): Previous silver USD price from DB.

    Returns:
        Optional[Tuple[Dict, Dict]]: Transformed data for gold and silver, or None if invalid.
    """
    if not validate_data(prices):
        return None
    
    timestamp = prices["timestamp"]
    
    gold_usd = prices["pax-gold"]
    gold_change = calculate_change(previous_gold or 0, gold_usd) if previous_gold else 0.0
    flag_extreme_movement("gold", timestamp, gold_change)
    
    silver_usd = prices["silver-token-xagx"]
    silver_change = calculate_change(previous_silver or 0, silver_usd) if previous_silver else 0.0
    flag_extreme_movement("silver", timestamp, silver_change)
    
    gold_data = {
        "date": timestamp,
        "metal": "gold",
        "price_usd": gold_usd,
        "price_change": gold_change
    }
    
    silver_data = {
        "date": timestamp,
        "metal": "silver",
        "price_usd": silver_usd,
        "price_change": silver_change
    }
    
    error_logger.info("Data transformation successful.")
    return gold_data, silver_data
