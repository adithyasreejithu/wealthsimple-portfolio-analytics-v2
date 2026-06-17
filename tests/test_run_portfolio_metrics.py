import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from run_portfolio_metrics import run_portfolio_metrics  # noqa: E402


class RunPortfolioMetricsTest(unittest.TestCase):
    def setUp(self):
        self.holdings = [
            {
                "ticker": "AAPL",
                "bucket": "Quality",
                "grouping_method": "stored_policy_result",
                "grouping_status": "grouped",
                "quantity": 2.0,
                "current_price": 150.0,
                "market_value": 300.0,
                "cost_basis": 200.0,
            },
            {
                "ticker": "XEQT",
                "bucket": "Core",
                "grouping_method": "manual_override",
                "grouping_status": "grouped",
                "quantity": 10.0,
                "current_price": 70.0,
                "market_value": 700.0,
                "cost_basis": 600.0,
            },
        ]
        self.history = [
            {"date": "2026-01-01", "portfolio_value": 1000.0},
            {"date": "2026-01-02", "portfolio_value": 1100.0},
        ]

    def test_portfolio_metrics_summary_can_be_exported(self):
        with patch("run_portfolio_metrics.export_metrics") as export_mock:
            metrics = run_portfolio_metrics(
                holdings=self.holdings,
                historical_values=self.history,
                cash_value=50.0,
                export_path="metrics.json",
            )

        export_mock.assert_called_once()
        self.assertEqual(metrics["type"], "portfolio_metrics_summary")
        self.assertEqual(metrics["portfolio_value"], 1050.0)
        self.assertIn("weights_by_ticker", metrics)
        self.assertIn("weights_by_bucket", metrics)

    def test_default_export_path_uses_exports_folder(self):
        with patch("run_portfolio_metrics.export_metrics") as export_mock:
            metrics = run_portfolio_metrics(
                holdings=self.holdings,
                historical_values=self.history,
                cash_value=50.0,
            )

        export_path = Path(metrics["export_path"])
        self.assertEqual(export_path.parent.name, "exports")
        self.assertEqual(export_path.name, "portfolio_metrics_summary.json")
        export_mock.assert_called_once()

    def test_single_ticker_metrics_can_be_generated_case_insensitively(self):
        with patch("run_portfolio_metrics.export_metrics"):
            metrics = run_portfolio_metrics(
                ticker="aapl",
                holdings=self.holdings,
                cash_value=50.0,
                export_path="metrics_AAPL.json",
            )

        self.assertEqual(metrics["type"], "position_metrics")
        self.assertEqual(metrics["ticker"], "AAPL")
        self.assertEqual(metrics["position"]["bucket"], "Quality")
        self.assertAlmostEqual(metrics["position_weight"]["weight"], 300.0 / 1050.0)
        self.assertAlmostEqual(metrics["unrealized_gain"]["unrealized_gain"], 100.0)

    def test_missing_ticker_raises_useful_error(self):
        with self.assertRaisesRegex(ValueError, "MSFT was not found"):
            run_portfolio_metrics(
                ticker="MSFT",
                holdings=self.holdings,
                cash_value=0.0,
                export=False,
            )


if __name__ == "__main__":
    unittest.main()
