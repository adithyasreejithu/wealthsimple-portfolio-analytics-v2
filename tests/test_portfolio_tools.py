import sys
import unittest
from pathlib import Path

import duckdb


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
sys.path.insert(0, str(SRC))

from portfolio_tools import get_current_holdings  # noqa: E402


def _create_holdings_schema(con):
    con.execute(
        """
        CREATE TABLE tickers (
            ticker_id BIGINT,
            ticker_symbol TEXT
        );
        CREATE TABLE stocks (
            ticker_id BIGINT,
            company_name TEXT,
            exchange TEXT,
            currency TEXT,
            sector TEXT,
            industry TEXT
        );
        CREATE TABLE etf (
            ticker_id BIGINT,
            asset TEXT,
            company_name TEXT,
            currency TEXT,
            fund_family TEXT
        );
        CREATE TABLE transactions (
            ticker_id BIGINT,
            transaction TEXT,
            quantity DOUBLE,
            debit DOUBLE,
            credit DOUBLE
        );
        CREATE TABLE HistoricalRecords (
            ticker_id BIGINT,
            date DATE,
            adj_close DOUBLE
        );
        """
    )
    con.execute("INSERT INTO tickers VALUES (1, 'AAPL');")
    con.execute(
        """
        INSERT INTO stocks VALUES (
            1,
            'Apple Inc.',
            'NASDAQ',
            'USD',
            'Technology',
            'Technology Hardware, Storage & Peripherals'
        );
        """
    )
    con.execute("INSERT INTO transactions VALUES (1, 'BUY', 2, 20, 0);")
    con.execute("INSERT INTO HistoricalRecords VALUES (1, DATE '2026-01-01', 15);")


class PortfolioToolsTest(unittest.TestCase):
    def test_current_holdings_use_active_grouping_table_when_available(self):
        con = duckdb.connect(":memory:")
        _create_holdings_schema(con)
        con.execute(
            """
            CREATE TABLE portfolio_grouping_active (
                ticker TEXT,
                portfolio_group TEXT,
                grouping_method TEXT,
                grouping_status TEXT,
                generated_at TIMESTAMP,
                source_policy_version TEXT,
                source_grouping_reference_version TEXT,
                sector TEXT,
                industry TEXT,
                security_type TEXT
            );
            """
        )
        con.execute(
            """
            INSERT INTO portfolio_grouping_active (
                ticker,
                portfolio_group,
                grouping_method,
                grouping_status
            )
            VALUES ('AAPL', 'Income', 'stored_policy_result', 'grouped');
            """
        )

        holdings = get_current_holdings(con=con)

        self.assertEqual(holdings[0]["bucket"], "Income")
        self.assertEqual(holdings[0]["grouping_method"], "stored_policy_result")
        self.assertEqual(holdings[0]["grouping_status"], "grouped")

    def test_current_holdings_fall_back_to_policy_rules_without_active_table(self):
        con = duckdb.connect(":memory:")
        _create_holdings_schema(con)

        holdings = get_current_holdings(con=con)

        self.assertEqual(holdings[0]["bucket"], "Quality")
        self.assertEqual(
            holdings[0]["grouping_method"],
            "industry_match:Technology Hardware & Equipment",
        )
        self.assertEqual(holdings[0]["grouping_status"], "grouped")

    def test_current_holdings_use_email_transaction_cost_when_debit_is_missing(self):
        con = duckdb.connect(":memory:")
        _create_holdings_schema(con)
        con.execute("UPDATE transactions SET debit = 0;")
        con.execute(
            """
            CREATE TABLE Email_Transactions (
                ticker_id BIGINT,
                ticker TEXT,
                transaction TEXT,
                quantity DOUBLE,
                total_cost DOUBLE,
                debit DOUBLE
            );
            """
        )
        con.execute(
            """
            INSERT INTO Email_Transactions VALUES
                (1, 'AAPL', 'Limit Buy', 2.0, 200.0, 200.0);
            """
        )

        holdings = get_current_holdings(con=con, source="email")

        self.assertAlmostEqual(holdings[0]["average_cost"], 100.0)
        self.assertAlmostEqual(holdings[0]["cost_basis"], 200.0)
        self.assertAlmostEqual(holdings[0]["unrealized_gain"], -170.0)
        self.assertEqual(holdings[0]["source"], "email")

    def test_current_holdings_transactions_remain_default_source(self):
        con = duckdb.connect(":memory:")
        _create_holdings_schema(con)
        con.execute(
            """
            CREATE TABLE Email_Transactions (
                ticker_id BIGINT,
                ticker TEXT,
                transaction TEXT,
                quantity DOUBLE,
                total_cost DOUBLE,
                debit DOUBLE
            );
            """
        )
        con.execute(
            """
            INSERT INTO Email_Transactions VALUES
                (1, 'AAPL', 'Limit Buy', 10.0, 1000.0, 1000.0);
            """
        )

        holdings = get_current_holdings(con=con)

        self.assertAlmostEqual(holdings[0]["quantity"], 2.0)
        self.assertAlmostEqual(holdings[0]["average_cost"], 10.0)
        self.assertEqual(holdings[0]["source"], "transactions")


if __name__ == "__main__":
    unittest.main()
