import sys
import unittest
from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from MonthlyFileExtract import clean_transactions  # noqa: E402


class MonthlyFileExtractTest(unittest.TestCase):
    def test_dividend_with_credit_and_missing_exec_date_is_kept(self):
        transactions = pd.DataFrame(
            [
                {
                    "date": "2024-01-15",
                    "transaction": "DIV",
                    "ticker_id": "PZA",
                    "quantity": None,
                    "execDate": None,
                    "fx_rate": None,
                    "debit": "$0.00",
                    "credit": "$0.31",
                    "balance": "$177.36",
                }
            ]
        )

        cleaned = clean_transactions(transactions)

        self.assertEqual(len(cleaned), 1)
        self.assertEqual(cleaned.loc[0, "transaction"], "DIV")
        self.assertEqual(cleaned.loc[0, "credit"], "$0.31")
        self.assertEqual(cleaned.loc[0, "execDate"], "2024-01-15")


if __name__ == "__main__":
    unittest.main()
