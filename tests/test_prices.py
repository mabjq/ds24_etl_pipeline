import unittest
import requests
from unittest.mock import patch, MagicMock, mock_open
import logging
from app.transform import validate_data, calculate_change, flag_extreme_movement
from app.api import fetch_metal_prices
from app.database import create_connection, get_latest_price, insert_price
from config.config import EXTREME_THRESHOLD

class TestPrices(unittest.TestCase):

    def test_validate_data(self):
        """Test validation of API data."""
        valid_data = {"pax-gold": 2000.0, "silver-token-xagx": 30.0, "timestamp": "2025-08-31T12:00:00"}
        self.assertTrue(validate_data(valid_data))
        
        invalid_data = {"pax-gold": "invalid", "silver-token-xagx": 30.0, "timestamp": "2025-08-31T12:00:00"}
        self.assertFalse(validate_data(invalid_data))
        
        missing_data = {"pax-gold": 2000.0}
        self.assertFalse(validate_data(missing_data))

    def test_calculate_change(self):
        """Test percentage change calculation."""
        self.assertEqual(calculate_change(1000, 1050), 5.0)
        self.assertEqual(calculate_change(1000, 950), -5.0)
        self.assertEqual(calculate_change(0, 1000), 0.0)  # Div by zero -> 0.0
        self.assertEqual(calculate_change("invalid", 1000), 0.0)  # Invalid -> 0.0

    def test_flag_extreme_movement(self):
        """Test flagging of extreme movements (check logging)."""
        with patch('logging.Logger.info') as mock_info:
            flag_extreme_movement("gold", "2025-08-31T12:00:00", 6.0)
            mock_info.assert_called_with("2025-08-31T12:00:00 - Extreme price movement for gold: +6.0%")
        
        with patch('logging.Logger.info') as mock_info:
            flag_extreme_movement("silver", "2025-08-31T12:00:00", -4.0)
            mock_info.assert_not_called()  # < threshold

    @patch('requests.get')
    def test_fetch_metal_prices_success(self, mock_get):
        """Test API fetching with mocked response."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "pax-gold": {"usd": 2000.0},
            "silver-token-xagx": {"usd": 30.0}
        }
        mock_get.return_value = mock_response
        
        prices = fetch_metal_prices()
        self.assertIsNotNone(prices)
        self.assertIn("pax-gold", prices)
        self.assertIn("timestamp", prices)

    @patch('requests.get')
    def test_fetch_metal_prices_failure(self, mock_get):
        """Test API error handling."""
        mock_get.side_effect = requests.exceptions.RequestException("API error")
        prices = fetch_metal_prices()
        self.assertIsNone(prices)

    @patch('sqlite3.connect')
    def test_get_latest_price(self, mock_connect):
        """Test fetching the latest price with a mocked DB."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = (2000.0, "2025-08-30T12:00:00")
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        result = get_latest_price(mock_conn, "gold")
        self.assertEqual(result, (2000.0, "2025-08-30T12:00:00"))

    @patch('sqlite3.connect')
    def test_insert_price(self, mock_connect):
        """Test insertion with a mocked DB."""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value = mock_cursor
        mock_connect.return_value = mock_conn
        
        with patch('app.database.get_latest_price', return_value=(2000.0, "2025-08-30T12:00:00")):
            inserted = insert_price(mock_conn, "2025-08-31T12:00:00", "gold", 2100.0, 5.0)
            self.assertTrue(inserted)
            mock_cursor.execute.assert_called()

if __name__ == '__main__':
    unittest.main()