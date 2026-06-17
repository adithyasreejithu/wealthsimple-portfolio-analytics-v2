import sys
import unittest
from pathlib import Path

import duckdb
import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from Database_Upload import upload_transactions  # noqa: E402


class DatabaseUploadTest(unittest.TestCase):
    def test_cash_credit_only_transaction_is_inserted(self):
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE cash_transactions (
                date DATE,
                transaction TEXT,
                execDate DATE,
                debit DOUBLE,
                credit DOUBLE,
                fxRate DOUBLE,
                balance DOUBLE,
                UNIQUE (date, transaction, execDate, debit, credit, fxRate, balance)
            );
            """
        )
        con.execute(
            """
            INSERT INTO cash_transactions
            VALUES (DATE '2026-01-15', 'FPLINT', DATE '2026-01-15', 0, 0, 0, 0);
            """
        )

        transactions = pd.DataFrame(
            [
                {
                    "Date": "2026-01-15",
                    "Type": "FPLINT",
                    "ExecDate": None,
                    "Debit": None,
                    "Credit": "$12.34",
                    "FXRate": None,
                    "Balance": "$112.34",
                }
            ]
        )

        upload_transactions(transactions, con)

        rows = con.execute(
            """
            SELECT transaction, debit, credit, balance
            FROM cash_transactions;
            """
        ).fetchall()

        self.assertEqual(rows, [("FPLINT", 0.0, 12.34, 112.34)])

    def test_security_dividend_currency_values_are_inserted(self):
        con = duckdb.connect(":memory:")
        con.execute(
            """
            CREATE TABLE tickers (
                ticker_id BIGINT,
                ticker_symbol TEXT
            );
            CREATE TABLE transactions (
                date DATE,
                transaction TEXT,
                ticker_id BIGINT,
                quantity DOUBLE,
                execDate DATE,
                debit DOUBLE,
                credit DOUBLE,
                fxRate DOUBLE,
                UNIQUE (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
            );
            """
        )
        con.execute("INSERT INTO tickers VALUES (1, 'PZA');")
        con.execute(
            """
            INSERT INTO transactions
            VALUES (DATE '2024-01-15', 'DIV', 1, 0, DATE '2024-01-15', 0, 0, 0);
            """
        )

        transactions = pd.DataFrame(
            [
                {
                    "date": "2024-01-15",
                    "transaction": "DIV",
                    "ticker_id": "PZA",
                    "quantity": None,
                    "execDate": "2024-01-15",
                    "fx_rate": None,
                    "debit": "$0.00",
                    "credit": "$0.31",
                    "balance": "$177.36",
                }
            ]
        )

        upload_transactions(transactions, con)

        rows = con.execute(
            """
            SELECT transaction, quantity, debit, credit
            FROM transactions;
            """
        ).fetchall()

        self.assertEqual(rows, [("DIV", 0.0, 0.0, 0.31)])


if __name__ == "__main__":
    unittest.main()
