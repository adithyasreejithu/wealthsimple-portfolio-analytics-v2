import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from run_portfolio_policy import run_policy_grouping  # noqa: E402


class RunPortfolioPolicyTest(unittest.TestCase):
    def setUp(self):
        self.holdings = [
            {
                "ticker": "AAPL",
                "security_name": "Apple Inc.",
                "security_type": "EQUITY",
                "sector": "Technology",
                "industry": "Technology Hardware, Storage & Peripherals",
            },
            {
                "ticker": "FAKE",
                "security_name": "Global Dividend Growth ETF",
                "security_type": "ETF",
                "etf_category": "Dividend Equity",
            },
        ]

    def test_all_holdings_are_exported_and_saved_to_db(self):
        con = duckdb.connect(":memory:")

        with patch("run_portfolio_policy.export_active_grouping") as export_mock:
            active_grouping = run_policy_grouping(
                holdings=self.holdings,
                con=con,
                export_path="active_policy.json",
            )

            rows = con.execute(
                """
                SELECT ticker, portfolio_group
                FROM portfolio_grouping_active
                ORDER BY ticker;
                """
            ).fetchall()

        export_mock.assert_called_once()
        self.assertEqual(len(active_grouping["holdings"]), 2)
        self.assertEqual(rows, [("AAPL", "Quality"), ("FAKE", "Income")])

    def test_default_export_path_uses_exports_folder(self):
        with patch("run_portfolio_policy.export_active_grouping") as export_mock:
            active_grouping = run_policy_grouping(
                holdings=self.holdings,
                save_to_db=False,
            )

        export_path = Path(active_grouping["export_path"])
        self.assertEqual(export_path.parent.name, "exports")
        self.assertEqual(export_path.name, "active_policy.json")
        export_mock.assert_called_once()

    def test_single_ticker_filters_case_insensitively(self):
        con = duckdb.connect(":memory:")

        with patch("run_portfolio_policy.export_active_grouping"):
            active_grouping = run_policy_grouping(
                ticker="aapl",
                holdings=self.holdings,
                con=con,
                export_path="active_policy_AAPL.json",
            )

        self.assertEqual(len(active_grouping["holdings"]), 1)
        self.assertEqual(active_grouping["holdings"][0]["ticker"], "AAPL")
        self.assertEqual(active_grouping["holdings"][0]["portfolio_group"], "Quality")

    def test_single_ticker_save_replaces_only_that_ticker(self):
        con = duckdb.connect(":memory:")

        with patch("run_portfolio_policy.export_active_grouping"):
            run_policy_grouping(
                holdings=self.holdings,
                con=con,
                export_path="active_policy.json",
            )
            run_policy_grouping(
                ticker="AAPL",
                holdings=self.holdings,
                con=con,
                export_path="active_policy_AAPL.json",
            )

            rows = con.execute(
                """
                SELECT ticker, portfolio_group
                FROM portfolio_grouping_active
                ORDER BY ticker;
                """
            ).fetchall()

        self.assertEqual(rows, [("AAPL", "Quality"), ("FAKE", "Income")])

    def test_missing_ticker_raises_useful_error(self):
        with self.assertRaisesRegex(ValueError, "MSFT was not found"):
            run_policy_grouping(
                ticker="MSFT",
                holdings=self.holdings,
                save_to_db=False,
            )


if __name__ == "__main__":
    unittest.main()
