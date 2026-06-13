"""
Clean data access tools for portfolio analytics.

The functions in this module retrieve and format data from the existing DuckDB
schema. They intentionally avoid heavy financial calculations; those belong in
portfolio_metrics.py.
"""

from __future__ import annotations

from contextlib import nullcontext
from typing import Any

import pandas as pd

from portfolio_policy import get_bucket_for_ticker, group_holding


def _connection_context(con=None, db_path: str | None = None):
    if con is not None:
        return nullcontext(con)

    from Database_Schema import get_connection

    if db_path is None:
        return get_connection()
    return get_connection(db_path)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    return float(value)


def _records(df: pd.DataFrame) -> list[dict]:
    return df.where(pd.notnull(df), None).to_dict(orient="records")


def get_current_holdings(con=None, db_path: str | None = None) -> list[dict]:
    """
    Return current security holdings with current price and market value.

    Share ownership is based on BUY minus SELL transactions. Loan, recall, and
    dividend rows do not change ownership.
    """
    query = """
        WITH latest_prices AS (
            SELECT ticker_id, date AS latest_price_date, adj_close AS current_price
            FROM (
                SELECT
                    ticker_id,
                    date,
                    adj_close,
                    ROW_NUMBER() OVER (
                        PARTITION BY ticker_id
                        ORDER BY date DESC
                    ) AS rn
                FROM HistoricalRecords
            )
            WHERE rn = 1
        ),
        transaction_totals AS (
            SELECT
                ticker_id,
                SUM(CASE WHEN transaction = 'BUY' THEN quantity ELSE 0 END) AS buy_quantity,
                SUM(CASE WHEN transaction = 'SELL' THEN quantity ELSE 0 END) AS sell_quantity,
                SUM(CASE WHEN transaction = 'BUY' THEN debit ELSE 0 END) AS total_buy_cost,
                SUM(CASE WHEN transaction = 'SELL' THEN credit ELSE 0 END) AS total_sell_proceeds
            FROM transactions
            GROUP BY ticker_id
        )
        SELECT
            t.ticker_symbol AS ticker,
            COALESCE(s.company_name, e.company_name) AS company_name,
            COALESCE(s.company_name, e.company_name) AS security_name,
            CASE
                WHEN e.ticker_id IS NOT NULL THEN 'ETF'
                WHEN s.ticker_id IS NOT NULL THEN 'EQUITY'
                ELSE NULL
            END AS security_type,
            COALESCE(s.currency, e.currency) AS currency,
            s.sector,
            s.industry,
            s.exchange,
            e.asset AS etf_category,
            e.fund_family,
            COALESCE(tt.buy_quantity, 0) - COALESCE(tt.sell_quantity, 0) AS quantity,
            COALESCE(lp.current_price, 0) AS current_price,
            lp.latest_price_date,
            COALESCE(tt.buy_quantity, 0) AS buy_quantity,
            COALESCE(tt.sell_quantity, 0) AS sell_quantity,
            COALESCE(tt.total_buy_cost, 0) AS total_buy_cost,
            COALESCE(tt.total_sell_proceeds, 0) AS total_sell_proceeds
        FROM transaction_totals tt
        JOIN tickers t
            ON t.ticker_id = tt.ticker_id
        LEFT JOIN stocks s
            ON s.ticker_id = t.ticker_id
        LEFT JOIN etf e
            ON e.ticker_id = t.ticker_id
        LEFT JOIN latest_prices lp
            ON lp.ticker_id = t.ticker_id
        WHERE COALESCE(tt.buy_quantity, 0) - COALESCE(tt.sell_quantity, 0) > 0
        ORDER BY t.ticker_symbol;
    """

    with _connection_context(con, db_path) as active_con:
        df = active_con.execute(query).fetchdf()

    if df.empty:
        return []

    df["ticker"] = df["ticker"].astype(str)
    grouped = df.apply(lambda row: group_holding(row.to_dict()), axis=1)
    df["bucket"] = grouped.apply(lambda row: row["portfolio_group"])
    df["grouping_method"] = grouped.apply(lambda row: row["grouping_method"])
    df["grouping_status"] = grouped.apply(lambda row: row["grouping_status"])
    df["quantity"] = df["quantity"].astype(float)
    df["current_price"] = df["current_price"].astype(float)
    df["market_value"] = df["quantity"] * df["current_price"]
    df["average_cost"] = df.apply(
        lambda row: (
            _safe_float(row["total_buy_cost"]) / _safe_float(row["buy_quantity"])
            if _safe_float(row["buy_quantity"]) > 0
            else 0.0
        ),
        axis=1,
    )
    df["cost_basis"] = df["average_cost"] * df["quantity"]
    df["unrealized_gain"] = df["market_value"] - df["cost_basis"]

    return _records(df)


def get_cash_available(con=None, db_path: str | None = None) -> dict:
    """Return the latest cash balance, falling back to net cash flow."""
    latest_balance_query = """
        SELECT balance
        FROM cash_transactions
        WHERE balance IS NOT NULL
        ORDER BY date DESC, id DESC
        LIMIT 1;
    """
    net_cash_query = """
        SELECT COALESCE(SUM(credit), 0) - COALESCE(SUM(debit), 0) AS cash_balance
        FROM cash_transactions;
    """

    with _connection_context(con, db_path) as active_con:
        latest = active_con.execute(latest_balance_query).fetchone()
        if latest and latest[0] is not None:
            balance = _safe_float(latest[0])
            source = "latest_balance"
        else:
            balance = _safe_float(active_con.execute(net_cash_query).fetchone()[0])
            source = "net_cash_flow"

    return {
        "ticker": "Cash",
        "bucket": "Cash",
        "cash_available": balance,
        "market_value": balance,
        "source": source,
    }


def get_portfolio_value(
    con=None,
    db_path: str | None = None,
    *,
    include_cash: bool = True,
) -> dict:
    """Return total portfolio value using current holdings and optional cash."""
    holdings = get_current_holdings(con=con, db_path=db_path)
    holdings_value = sum(_safe_float(row.get("market_value")) for row in holdings)
    cash = get_cash_available(con=con, db_path=db_path) if include_cash else {}
    cash_value = _safe_float(cash.get("cash_available")) if include_cash else 0.0

    return {
        "portfolio_value": holdings_value + cash_value,
        "holdings_value": holdings_value,
        "cash_value": cash_value,
        "include_cash": include_cash,
    }


def get_allocation_by_ticker(con=None, db_path: str | None = None) -> dict:
    """Return current allocation weights by ticker."""
    holdings = get_current_holdings(con=con, db_path=db_path)
    total_value = sum(_safe_float(row.get("market_value")) for row in holdings)

    if total_value <= 0:
        return {}

    return {
        row["ticker"]: {
            "ticker": row["ticker"],
            "bucket": row["bucket"],
            "market_value": _safe_float(row.get("market_value")),
            "weight": _safe_float(row.get("market_value")) / total_value,
        }
        for row in holdings
    }


def get_allocation_by_bucket(
    con=None,
    db_path: str | None = None,
    *,
    include_cash: bool = True,
) -> dict:
    """Return current allocation weights by policy bucket."""
    holdings = get_current_holdings(con=con, db_path=db_path)
    bucket_values: dict[str, float] = {}

    for row in holdings:
        bucket = row.get("bucket", "Unclassified")
        bucket_values[bucket] = bucket_values.get(bucket, 0.0) + _safe_float(
            row.get("market_value")
        )

    if include_cash:
        cash = get_cash_available(con=con, db_path=db_path)
        bucket_values["Cash"] = bucket_values.get("Cash", 0.0) + _safe_float(
            cash.get("cash_available")
        )

    total_value = sum(bucket_values.values())
    if total_value <= 0:
        return {}

    return {
        bucket: {
            "bucket": bucket,
            "market_value": value,
            "weight": value / total_value,
        }
        for bucket, value in sorted(bucket_values.items())
    }


def get_position_summary(
    ticker: str,
    con=None,
    db_path: str | None = None,
) -> dict:
    """Return a single holding summary by ticker."""
    normalized = str(ticker).upper().replace(".TO", "").strip()
    holdings = get_current_holdings(con=con, db_path=db_path)

    for row in holdings:
        if str(row.get("ticker", "")).upper().replace(".TO", "") == normalized:
            return row

    return {
        "ticker": normalized,
        "bucket": get_bucket_for_ticker(normalized),
        "quantity": 0.0,
        "current_price": 0.0,
        "market_value": 0.0,
        "cost_basis": 0.0,
        "unrealized_gain": 0.0,
    }


def get_historical_values(con=None, db_path: str | None = None) -> list[dict]:
    """
    Return daily invested-portfolio values from stored price history.

    The result excludes cash because the current schema stores cash balances as
    transaction snapshots rather than a full daily cash history.
    """
    transactions_query = """
        SELECT
            t.ticker_symbol AS ticker,
            COALESCE(tr.execDate, tr.date) AS date,
            SUM(
                CASE
                    WHEN tr.transaction = 'BUY' THEN tr.quantity
                    WHEN tr.transaction = 'SELL' THEN -tr.quantity
                    ELSE 0
                END
            ) AS signed_quantity
        FROM transactions tr
        JOIN tickers t
            ON t.ticker_id = tr.ticker_id
        WHERE tr.transaction IN ('BUY', 'SELL')
        GROUP BY t.ticker_symbol, COALESCE(tr.execDate, tr.date);
    """
    prices_query = """
        SELECT
            t.ticker_symbol AS ticker,
            h.date,
            h.adj_close
        FROM HistoricalRecords h
        JOIN tickers t
            ON t.ticker_id = h.ticker_id
        ORDER BY h.date, t.ticker_symbol;
    """

    with _connection_context(con, db_path) as active_con:
        transactions = active_con.execute(transactions_query).fetchdf()
        prices = active_con.execute(prices_query).fetchdf()

    if transactions.empty or prices.empty:
        return []

    transactions["date"] = pd.to_datetime(transactions["date"])
    prices["date"] = pd.to_datetime(prices["date"])

    price_pivot = prices.pivot_table(
        index="date",
        columns="ticker",
        values="adj_close",
        aggfunc="last",
    ).sort_index().ffill()
    quantity_pivot = transactions.pivot_table(
        index="date",
        columns="ticker",
        values="signed_quantity",
        aggfunc="sum",
    ).sort_index()

    all_dates = price_pivot.index.union(quantity_pivot.index).sort_values()
    quantity_pivot = quantity_pivot.reindex(all_dates).fillna(0).cumsum()
    quantity_pivot = quantity_pivot.reindex(price_pivot.index).ffill().fillna(0)
    quantity_pivot = quantity_pivot.reindex(columns=price_pivot.columns).fillna(0)
    value_pivot = quantity_pivot * price_pivot
    total_values = value_pivot.sum(axis=1)

    return [
        {
            "date": index.date().isoformat(),
            "portfolio_value": float(value),
        }
        for index, value in total_values.items()
    ]
