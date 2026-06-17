import sys
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from yfinance_gather_security_info import get_security_history  # noqa: E402


class SecurityHistoryTest(unittest.TestCase):
    def test_empty_ticker_list_skips_yfinance_download(self):
        with (
            patch(
                "yfinance_gather_security_info.get_all_tickers",
                return_value=pd.DataFrame({"final_ticker": []}),
            ),
            patch(
                "yfinance_gather_security_info.get_last_date_stored",
                return_value=None,
            ),
            patch("yfinance_gather_security_info.yf.download") as download_mock,
        ):
            result = get_security_history(None)

        self.assertTrue(result.empty)
        download_mock.assert_not_called()


if __name__ == "__main__":
    unittest.main()
