import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from portfolio_metrics import (  # noqa: E402
    calculate_allocation_drift,
    calculate_bucket_weights,
    calculate_max_drawdown,
    calculate_position_weights,
    calculate_sharpe_ratio,
    calculate_total_return,
    calculate_unrealized_gain_percent,
    calculate_volatility,
    check_bucket_limits,
    check_position_limits,
    generate_portfolio_metrics_summary,
    recommend_contribution_allocation,
)
from portfolio_policy import generate_active_grouping, group_holding  # noqa: E402


class PortfolioMetricsTest(unittest.TestCase):
    def setUp(self):
        self.holdings = [
            {
                "ticker": "XEQT",
                "bucket": "Core",
                "market_value": 700.0,
                "cost_basis": 600.0,
            },
            {
                "ticker": "SCHD",
                "bucket": "Income",
                "market_value": 150.0,
                "cost_basis": 140.0,
            },
            {
                "ticker": "PLTR",
                "bucket": "Growth",
                "market_value": 150.0,
                "cost_basis": 100.0,
            },
        ]
        self.history = [
            {"date": "2026-01-01", "portfolio_value": 100.0},
            {"date": "2026-01-02", "portfolio_value": 110.0},
            {"date": "2026-01-03", "portfolio_value": 105.0},
            {"date": "2026-01-04", "portfolio_value": 120.0},
        ]

    def test_current_allocation_can_be_calculated(self):
        weights = calculate_position_weights(self.holdings)

        self.assertAlmostEqual(weights["XEQT"]["weight"], 0.70)
        self.assertAlmostEqual(weights["SCHD"]["weight"], 0.15)
        self.assertAlmostEqual(weights["PLTR"]["weight"], 0.15)

    def test_bucket_allocation_can_be_calculated(self):
        weights = calculate_bucket_weights(self.holdings)

        self.assertAlmostEqual(weights["Core"]["weight"], 0.70)
        self.assertAlmostEqual(weights["Income"]["weight"], 0.15)
        self.assertAlmostEqual(weights["Growth"]["weight"], 0.15)

    def test_allocation_drift_can_be_calculated(self):
        bucket_weights = calculate_bucket_weights(self.holdings)
        drift = calculate_allocation_drift(bucket_weights)

        self.assertAlmostEqual(drift["Core"]["drift"], 0.0)
        self.assertEqual(drift["Core"]["status"], "on_target")

    def test_unrealized_gains_can_be_calculated(self):
        gain = calculate_unrealized_gain_percent(self.holdings[0])

        self.assertAlmostEqual(gain["unrealized_gain"], 100.0)
        self.assertAlmostEqual(gain["unrealized_gain_percent"], 100.0 / 600.0)

    def test_risk_metrics_can_be_calculated(self):
        total_return = calculate_total_return(self.history)
        volatility = calculate_volatility(self.history)
        drawdown = calculate_max_drawdown(self.history)
        sharpe = calculate_sharpe_ratio(self.history)

        self.assertAlmostEqual(total_return["total_return"], 0.20)
        self.assertGreater(volatility["volatility"], 0)
        self.assertLess(drawdown["max_drawdown"], 0)
        self.assertIn("sharpe_ratio", sharpe)

    def test_risk_metrics_are_order_independent(self):
        shuffled_history = [
            self.history[2],
            self.history[0],
            self.history[3],
            self.history[1],
        ]

        sorted_total_return = calculate_total_return(self.history)
        sorted_volatility = calculate_volatility(self.history)
        sorted_drawdown = calculate_max_drawdown(self.history)
        sorted_sharpe = calculate_sharpe_ratio(self.history)

        shuffled_total_return = calculate_total_return(shuffled_history)
        shuffled_volatility = calculate_volatility(shuffled_history)
        shuffled_drawdown = calculate_max_drawdown(shuffled_history)
        shuffled_sharpe = calculate_sharpe_ratio(shuffled_history)

        self.assertAlmostEqual(
            shuffled_total_return["total_return"],
            sorted_total_return["total_return"],
        )
        self.assertAlmostEqual(
            shuffled_volatility["volatility"],
            sorted_volatility["volatility"],
        )
        self.assertAlmostEqual(
            shuffled_drawdown["max_drawdown"],
            sorted_drawdown["max_drawdown"],
        )
        self.assertAlmostEqual(
            shuffled_sharpe["sharpe_ratio"],
            sorted_sharpe["sharpe_ratio"],
        )

    def test_contribution_recommendations_can_be_generated(self):
        bucket_weights = {
            "Core": {"bucket": "Core", "market_value": 620.0, "weight": 0.62},
            "Income": {
                "bucket": "Income",
                "market_value": 150.0,
                "weight": 0.15,
            },
            "Growth": {
                "bucket": "Growth",
                "market_value": 230.0,
                "weight": 0.23,
            },
        }

        recommendation = recommend_contribution_allocation(100.0, bucket_weights)

        self.assertIn("Core", recommendation["allocations"])
        self.assertNotIn("Growth", recommendation["allocations"])
        self.assertEqual(recommendation["allocations"]["Core"]["ticker"], "XEQT")
        self.assertTrue(any("Growth" in note for note in recommendation["notes"]))

    def test_limit_warnings_can_be_generated(self):
        position_weights = calculate_position_weights(self.holdings)
        bucket_weights = {
            "Core": {"bucket": "Core", "market_value": 590.0, "weight": 0.59},
            "Growth": {
                "bucket": "Growth",
                "market_value": 210.0,
                "weight": 0.21,
            },
        }

        position_warnings = check_position_limits(position_weights)
        bucket_warnings = check_bucket_limits(bucket_weights)

        self.assertTrue(any(w["ticker"] == "PLTR" for w in position_warnings))
        self.assertTrue(any(w["bucket"] == "Core" for w in bucket_warnings))
        self.assertTrue(any(w["bucket"] == "Growth" for w in bucket_warnings))

    def test_summary_output_is_valid_and_ai_readable(self):
        summary = generate_portfolio_metrics_summary(
            contribution_amount=100.0,
            holdings=self.holdings,
            historical_values=self.history,
        )

        self.assertEqual(summary["portfolio_value"], 1000.0)
        self.assertIn("weights_by_ticker", summary)
        self.assertIn("weights_by_bucket", summary)
        self.assertIn("allocation_drift", summary)
        self.assertIn("unrealized_gains", summary)
        self.assertIn("risk_metrics", summary)
        self.assertIsInstance(summary["warnings"], list)
        self.assertIn("recommended_contribution_allocation", summary)

    def test_stock_grouping_uses_sector_and_industry_metadata(self):
        grouped = group_holding(
            {
                "ticker": "AAPL",
                "security_name": "Apple Inc.",
                "security_type": "EQUITY",
                "sector": "Technology",
                "industry": "Technology Hardware, Storage & Peripherals",
            }
        )

        self.assertEqual(grouped["portfolio_group"], "Quality")
        self.assertEqual(grouped["grouping_status"], "grouped")

    def test_etf_grouping_uses_name_and_category_keywords(self):
        grouped = group_holding(
            {
                "ticker": "FAKE",
                "security_name": "Global Dividend Growth ETF",
                "security_type": "ETF",
                "etf_category": "Dividend Equity",
            }
        )

        self.assertEqual(grouped["portfolio_group"], "Income")
        self.assertEqual(grouped["grouping_status"], "grouped")

    def test_active_grouping_collects_missing_metadata_warnings(self):
        active_grouping = generate_active_grouping(
            holdings=[
                {
                    "ticker": "UNKNOWN",
                    "security_name": "Unknown Co",
                    "security_type": "EQUITY",
                }
            ]
        )

        self.assertEqual(
            active_grouping["holdings"][0]["grouping_status"],
            "needs_review",
        )
        self.assertTrue(any("Missing sector" in w for w in active_grouping["warnings"]))


if __name__ == "__main__":
    unittest.main()
