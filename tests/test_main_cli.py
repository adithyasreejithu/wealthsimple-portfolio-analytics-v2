import importlib
import json
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

main_module = importlib.import_module("main")


class MainCliTest(unittest.TestCase):
    def test_holdings_runs_holdings_only(self):
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
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "get_current_holdings",
                return_value=holdings,
            ) as holdings_mock,
            patch.object(main_module, "format_holdings_table", return_value="holdings"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--holdings"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_not_called()
        holdings_mock.assert_called_once_with(
            db_path=None,
            source="transactions",
        )

    def test_update_holdings_runs_pipeline_then_holdings(self):
        with (
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "get_current_holdings",
                return_value=[],
            ) as holdings_mock,
            patch.object(main_module, "format_holdings_table", return_value="holdings"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--update-holdings"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_called_once_with(db_path=None)
        holdings_mock.assert_called_once_with(
            db_path=None,
            source="transactions",
        )

    def test_holdings_accepts_email_holding_source(self):
        with (
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "get_current_holdings",
                return_value=[],
            ) as holdings_mock,
            patch.object(main_module, "format_holdings_table", return_value="holdings"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--holdings", "--holding-source", "email"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_not_called()
        holdings_mock.assert_called_once_with(
            db_path=None,
            source="email",
        )

    def test_holdings_accepts_single_ticker(self):
        holdings = [
            {"ticker": "AAPL", "source": "transactions"},
            {"ticker": "XEQT", "source": "transactions"},
        ]

        with (
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "get_current_holdings",
                return_value=holdings,
            ),
            patch.object(
                main_module,
                "format_holdings_table",
                return_value="filtered holdings",
            ) as format_mock,
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--holdings", "--ticker", "AAPL"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_not_called()
        format_mock.assert_called_once_with([holdings[0]])

    def test_holdings_can_print_json(self):
        holdings = [
            {
                "ticker": "AAPL",
                "source": "transactions",
                "quantity": 2.0,
            }
        ]

        with (
            patch.object(main_module, "run_data_pipeline"),
            patch.object(
                main_module,
                "get_current_holdings",
                return_value=holdings,
            ),
            patch.object(main_module, "close_connection"),
            patch("builtins.print") as print_mock,
        ):
            exit_code = main_module.main(["--holdings", "--json"])

        self.assertEqual(exit_code, 0)
        printed = json.loads(print_mock.call_args.args[0])
        self.assertEqual(printed, holdings)

    def test_json_requires_holdings_mode(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as exc:
            main_module.main(["--json"])

        self.assertEqual(exc.exception.code, 2)

    def test_email_holding_source_requires_holdings_mode(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as exc:
            main_module.main(["--holding-source", "email"])

        self.assertEqual(exc.exception.code, 2)

    def test_policy_runs_grouping_only(self):
        active_grouping = {"holdings": [], "export_path": "exports/active_policy.json"}

        with (
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "run_policy_grouping",
                return_value=active_grouping,
            ) as policy_mock,
            patch.object(main_module, "format_grouping_table", return_value="table"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--policy"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_not_called()
        policy_mock.assert_called_once_with(
            ticker=None,
            db_path=None,
            export_path=None,
        )

    def test_update_policy_runs_pipeline_then_grouping(self):
        active_grouping = {"holdings": [], "export_path": "exports/active_policy.json"}

        with (
            patch.object(main_module, "run_data_pipeline") as pipeline_mock,
            patch.object(
                main_module,
                "run_policy_grouping",
                return_value=active_grouping,
            ) as policy_mock,
            patch.object(main_module, "format_grouping_table", return_value="table"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--update-policy"])

        self.assertEqual(exit_code, 0)
        pipeline_mock.assert_called_once_with(db_path=None)
        policy_mock.assert_called_once_with(
            ticker=None,
            db_path=None,
            export_path=None,
        )

    def test_update_policy_accepts_single_ticker(self):
        active_grouping = {"holdings": [], "export_path": "exports/active_policy_AAPL.json"}

        with (
            patch.object(main_module, "run_data_pipeline"),
            patch.object(
                main_module,
                "run_policy_grouping",
                return_value=active_grouping,
            ) as policy_mock,
            patch.object(main_module, "format_grouping_table", return_value="table"),
            patch.object(main_module, "close_connection"),
            patch("builtins.print"),
        ):
            exit_code = main_module.main(["--update-policy", "--ticker", "AAPL"])

        self.assertEqual(exit_code, 0)
        policy_mock.assert_called_once_with(
            ticker="AAPL",
            db_path=None,
            export_path=None,
        )

    def test_metrics_flags_are_not_registered(self):
        with patch("sys.stderr"), self.assertRaises(SystemExit) as exc:
            main_module.main(["--metrics"])

        self.assertEqual(exc.exception.code, 2)


if __name__ == "__main__":
    unittest.main()
