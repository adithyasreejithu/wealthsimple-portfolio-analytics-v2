from __future__ import annotations

from typing import Any

from portfolio_policy import normalize_ticker


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(value)


def _filter_holdings_by_ticker(holdings: list[dict], ticker: str) -> list[dict]:
    normalized = normalize_ticker(ticker)
    return [
        holding
        for holding in holdings
        if normalize_ticker(holding.get("ticker")) == normalized
    ]


def format_holdings_table(holdings: list[dict]) -> str:
    if not holdings:
        return "No current holdings found."

    columns = [
        ("ticker", "Ticker"),
        ("source", "Source"),
        ("quantity", "Quantity"),
        ("current_price", "Price"),
        ("market_value", "Market Value"),
        ("average_cost", "Avg Cost"),
        ("bucket", "Bucket"),
    ]
    rows = []
    for holding in holdings:
        rows.append(
            {
                "ticker": holding.get("ticker"),
                "source": holding.get("source", "transactions"),
                "quantity": f"{_safe_float(holding.get('quantity')):,.6f}",
                "current_price": f"${_safe_float(holding.get('current_price')):,.2f}",
                "market_value": f"${_safe_float(holding.get('market_value')):,.2f}",
                "average_cost": f"${_safe_float(holding.get('average_cost')):,.2f}",
                "bucket": holding.get("bucket"),
            }
        )

    widths = {
        key: max(len(title), *(len(str(row.get(key) or "")) for row in rows))
        for key, title in columns
    }
    header = " | ".join(title.ljust(widths[key]) for key, title in columns)
    divider = "-+-".join("-" * widths[key] for key, _ in columns)
    body = [
        " | ".join(str(row.get(key) or "").ljust(widths[key]) for key, _ in columns)
        for row in rows
    ]
    return "\n".join([header, divider, *body])

