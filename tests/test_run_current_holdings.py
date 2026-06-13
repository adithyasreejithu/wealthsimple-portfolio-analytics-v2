import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

import run_current_holdings  # noqa: E402


class RunCurrentHoldingsTest(unittest.TestCase):
    def test_current_holdings_default_to_transactions_source(self):
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

        with (
            patch.object(
                run_current_holdings,
                "get_current_holdings",
                return_value=holdings,
            ) as holdings_mock,
            patch.object(run_current_holdings, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = run_current_holdings.main([])

        self.assertEqual(exit_code, 0)
        holdings_mock.assert_called_once_with(
            db_path=None,
            source="transactions",
        )

    def test_current_holdings_accept_email_source(self):
        with (
            patch.object(
                run_current_holdings,
                "get_current_holdings",
                return_value=[],
            ) as holdings_mock,
            patch.object(run_current_holdings, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = run_current_holdings.main(["--holding-source", "email"])

        self.assertEqual(exit_code, 0)
        holdings_mock.assert_called_once_with(
            db_path=None,
            source="email",
        )


if __name__ == "__main__":
    unittest.main()
