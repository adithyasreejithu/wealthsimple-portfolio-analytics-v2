import importlib
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

main_module = importlib.import_module("main")


class MainCliTest(unittest.TestCase):
    def test_policy_runs_grouping_only(self):
        active_grouping = {"holdings": [], "export_path": "ref/active_policy.json"}

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
        active_grouping = {"holdings": [], "export_path": "ref/active_policy.json"}

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
        active_grouping = {"holdings": [], "export_path": "ref/active_policy_AAPL.json"}

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


if __name__ == "__main__":
    unittest.main()
