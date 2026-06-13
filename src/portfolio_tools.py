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

from portfolio_policy import get_bucket_for_ticker, group_holding, normalize_ticker

HOLDING_SOURCES = ("transactions", "email")


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


def _table_exists(con, table_name: str) -> bool:
    rows = con.execute("SHOW TABLES;").fetchall()
    return table_name in {row[0] for row in rows}


def _normalize_holding_source(source: str) -> str:
    normalized = str(source).strip().lower()
    if normalized not in HOLDING_SOURCES:
        raise ValueError(
            f"holding source must be one of {', '.join(HOLDING_SOURCES)}"
        )
    return normalized


def _active_grouping_by_ticker(con) -> dict[str, dict]:
    if not _table_exists(con, "portfolio_grouping_active"):
        return {}

    df = con.execute(
        """
        SELECT
            ticker,
            portfolio_group,
            grouping_method,
            grouping_status
        FROM portfolio_grouping_active;
        """
    ).fetchdf()

    if df.empty:
        return {}

    return {
        normalize_ticker(row["ticker"]): row
        for row in df.where(pd.notnull(df), None).to_dict(orient="records")
    }


def _grouping_for_row(row: dict, active_grouping: dict[str, dict]) -> dict:
    stored_grouping = active_grouping.get(normalize_ticker(row.get("ticker")))
    if stored_grouping:
        return {
            "portfolio_group": stored_grouping.get("portfolio_group"),
            "grouping_method": stored_grouping.get("grouping_method"),
            "grouping_status": stored_grouping.get("grouping_status"),
        }

    return group_holding(row)


def _holding_totals_cte(source: str) -> str:
    if source == "transactions":
        return """
        holding_totals AS (
            SELECT
                tr.ticker_id,
                t.ticker_symbol,
                SUM(COALESCE(tr.quantity, 0)) AS raw_total_quantity,
                SUM(COALESCE(tr.quantity, 0)) FILTER (
                    WHERE tr.transaction = 'BUY'
                ) AS buy_quantity,
                COUNT_IF(tr.transaction = 'BUY') AS buy_count,
                SUM(COALESCE(tr.quantity, 0)) FILTER (
                    WHERE tr.transaction = 'SELL'
                ) AS sell_quantity,
                COUNT_IF(tr.transaction = 'SELL') AS sell_count,
                SUM(COALESCE(tr.debit, 0)) FILTER (
                    WHERE tr.transaction = 'BUY'
                ) AS total_buy_cost,
                SUM(COALESCE(tr.credit, 0)) FILTER (
                    WHERE tr.transaction = 'SELL'
                ) AS total_sell_proceeds
            FROM transactions tr
            JOIN tickers t
                ON tr.ticker_id = t.ticker_id
            GROUP BY tr.ticker_id, t.ticker_symbol
        )
        """

    return """
        holding_totals AS (
            SELECT
                COALESCE(t.ticker_id, et.ticker_id) AS ticker_id,
                COALESCE(t.ticker_symbol, et.ticker) AS ticker_symbol,
                SUM(COALESCE(et.quantity, 0)) AS raw_total_quantity,
                SUM(COALESCE(et.quantity, 0)) FILTER (
                    WHERE et.transaction ILIKE '%buy%'
                ) AS buy_quantity,
                COUNT_IF(et.transaction ILIKE '%buy%') AS buy_count,
                SUM(COALESCE(et.quantity, 0)) FILTER (
                    WHERE et.transaction ILIKE '%sell%'
                ) AS sell_quantity,
                COUNT_IF(et.transaction ILIKE '%sell%') AS sell_count,
                SUM(COALESCE(et.total_cost, et.debit, 0)) FILTER (
                    WHERE et.transaction ILIKE '%buy%'
                ) AS total_buy_cost,
                SUM(COALESCE(et.total_cost, et.debit, 0)) FILTER (
                    WHERE et.transaction ILIKE '%sell%'
                ) AS total_sell_proceeds
            FROM Email_Transactions et
            LEFT JOIN tickers t
                ON t.ticker_id = et.ticker_id
                OR t.ticker_symbol = et.ticker
            GROUP BY
                COALESCE(t.ticker_id, et.ticker_id),
                COALESCE(t.ticker_symbol, et.ticker)
        )
        """


def get_current_holdings(
    con=None,
    db_path: str | None = None,
    *,
    source: str = "transactions",
) -> list[dict]:
    """
    Return current security holdings with current price and market value.

    Share ownership is based on BUY minus SELL rows from the selected source.
    Loan, recall, and dividend rows do not change ownership.
    """
    source = _normalize_holding_source(source)
    with _connection_context(con, db_path) as active_con:
        if source == "email" and not _table_exists(active_con, "Email_Transactions"):
            raise ValueError("Email_Transactions table does not exist")

        query = f"""
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
        {_holding_totals_cte(source)}
        SELECT
            COALESCE(t.ticker_symbol, ht.ticker_symbol) AS ticker,
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
            COALESCE(ht.buy_quantity, 0) - COALESCE(ht.sell_quantity, 0) AS quantity,
            COALESCE(ht.raw_total_quantity, 0) AS raw_total_quantity,
            COALESCE(lp.current_price, 0) AS current_price,
            lp.latest_price_date,
            COALESCE(ht.buy_quantity, 0) AS buy_quantity,
            COALESCE(ht.buy_count, 0) AS buy_count,
            COALESCE(ht.sell_quantity, 0) AS sell_quantity,
            COALESCE(ht.sell_count, 0) AS sell_count,
            COALESCE(ht.total_buy_cost, 0) AS total_buy_cost,
            COALESCE(ht.buy_quantity, 0) AS cost_basis_quantity,
            COALESCE(ht.total_sell_proceeds, 0) AS total_sell_proceeds,
            '{source}' AS source
        FROM holding_totals ht
        LEFT JOIN tickers t
            ON t.ticker_id = ht.ticker_id
            OR t.ticker_symbol = ht.ticker_symbol
        LEFT JOIN stocks s
            ON s.ticker_id = t.ticker_id
        LEFT JOIN etf e
            ON e.ticker_id = t.ticker_id
        LEFT JOIN latest_prices lp
            ON lp.ticker_id = t.ticker_id
        WHERE COALESCE(ht.buy_quantity, 0) - COALESCE(ht.sell_quantity, 0) > 0
        ORDER BY COALESCE(t.ticker_symbol, ht.ticker_symbol);
    """
        df = active_con.execute(query).fetchdf()
        active_grouping = _active_grouping_by_ticker(active_con)

    if df.empty:
        return []

    df["ticker"] = df["ticker"].astype(str)
    grouped = df.apply(
        lambda row: _grouping_for_row(row.to_dict(), active_grouping),
        axis=1,
    )
    df["bucket"] = grouped.apply(lambda row: row["portfolio_group"])
    df["grouping_method"] = grouped.apply(lambda row: row["grouping_method"])
    df["grouping_status"] = grouped.apply(lambda row: row["grouping_status"])
    df["quantity"] = df["quantity"].astype(float)
    df["current_price"] = df["current_price"].astype(float)
    df["market_value"] = df["quantity"] * df["current_price"]
    df["average_cost"] = df.apply(
        lambda row: (
            _safe_float(row["total_buy_cost"]) / _safe_float(row["cost_basis_quantity"])
            if _safe_float(row["cost_basis_quantity"]) > 0
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
    source: str = "transactions",
) -> dict:
    """Return total portfolio value using current holdings and optional cash."""
    holdings = get_current_holdings(con=con, db_path=db_path, source=source)
    holdings_value = sum(_safe_float(row.get("market_value")) for row in holdings)
    cash = get_cash_available(con=con, db_path=db_path) if include_cash else {}
    cash_value = _safe_float(cash.get("cash_available")) if include_cash else 0.0

    return {
        "portfolio_value": holdings_value + cash_value,
        "holdings_value": holdings_value,
        "cash_value": cash_value,
        "include_cash": include_cash,
        "source": _normalize_holding_source(source),
    }


def get_allocation_by_ticker(
    con=None,
    db_path: str | None = None,
    *,
    source: str = "transactions",
) -> dict:
    """Return current allocation weights by ticker."""
    holdings = get_current_holdings(con=con, db_path=db_path, source=source)
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
    source: str = "transactions",
) -> dict:
    """Return current allocation weights by policy bucket."""
    holdings = get_current_holdings(con=con, db_path=db_path, source=source)
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
    *,
    source: str = "transactions",
) -> dict:
    """Return a single holding summary by ticker."""
    normalized = str(ticker).upper().replace(".TO", "").strip()
    holdings = get_current_holdings(con=con, db_path=db_path, source=source)

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


def get_historical_values(
    con=None,
    db_path: str | None = None,
    *,
    source: str = "transactions",
) -> list[dict]:
    """
    Return daily invested-portfolio values from stored price history.

    The result excludes cash because the current schema stores cash balances as
    transaction snapshots rather than a full daily cash history.
    """
    source = _normalize_holding_source(source)
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
    email_transactions_query = """
        SELECT
            COALESCE(t.ticker_symbol, et.ticker) AS ticker,
            et.date,
            SUM(
                CASE
                    WHEN et.transaction ILIKE '%buy%' THEN et.quantity
                    WHEN et.transaction ILIKE '%sell%' THEN -et.quantity
                    ELSE 0
                END
            ) AS signed_quantity
        FROM Email_Transactions et
        LEFT JOIN tickers t
            ON t.ticker_id = et.ticker_id
            OR t.ticker_symbol = et.ticker
        WHERE et.transaction ILIKE '%buy%' OR et.transaction ILIKE '%sell%'
        GROUP BY COALESCE(t.ticker_symbol, et.ticker), et.date;
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
        if source == "email" and not _table_exists(active_con, "Email_Transactions"):
            raise ValueError("Email_Transactions table does not exist")

        source_query = (
            transactions_query
            if source == "transactions"
            else email_transactions_query
        )
        transactions = active_con.execute(source_query).fetchdf()
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
