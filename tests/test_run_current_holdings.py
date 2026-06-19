import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import run_current_holdings  # noqa: E402


class RunCurrentHoldingsTest(unittest.TestCase):
    def test_filter_holdings_by_ticker_is_case_insensitive(self):
        holdings = [
            {
                "ticker": "AAPL",
                "source": "transactions",
            },
            {
                "ticker": "XEQT.TO",
                "source": "transactions",
            },
        ]

        filtered = run_current_holdings._filter_holdings_by_ticker(holdings, "xeqt")

        self.assertEqual(filtered, [holdings[1]])

    def test_format_holdings_table_includes_core_columns(self):
        holdings = [
            {
                "ticker": "AAPL",
                "source": "transactions",
                "quantity": 2.0,
                "current_price": 150.0,
                "market_value": 300.0,
                "average_cost": 100.0,
                "bucket": "Quality",
            }
        ]

        table = run_current_holdings.format_holdings_table(holdings)

        self.assertIn("Ticker", table)
        self.assertIn("Market Value", table)
        self.assertIn("AAPL", table)
        self.assertIn("Quality", table)


if __name__ == "__main__":
    unittest.main()
