from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from Database_Schema import close_connection
from portfolio_policy import normalize_ticker
from portfolio_tools import get_current_holdings


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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Show current holdings from DuckDB.",
    )
    parser.add_argument(
        "--db-path",
        help="DuckDB path. Defaults to Database_Schema.DB_PATH.",
    )
    parser.add_argument(
        "--ticker",
        help="Only show one ticker, for example AAPL.",
    )
    parser.add_argument(
        "--holding-source",
        choices=["transactions", "email"],
        default="transactions",
        help="Holding source. Defaults to transactions.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print holdings as JSON.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        holdings = get_current_holdings(
            db_path=args.db_path,
            source=args.holding_source,
        )
        if args.ticker:
            holdings = _filter_holdings_by_ticker(holdings, args.ticker)
            if not holdings:
                print(
                    f"{normalize_ticker(args.ticker)} was not found in current holdings",
                    file=sys.stderr,
                )
                return 1
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Current holdings failed: {exc}", file=sys.stderr)
        return 1
    finally:
        close_connection()

    if args.json:
        print(json.dumps(holdings, indent=2, default=str))
    else:
        print(format_holdings_table(holdings))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
