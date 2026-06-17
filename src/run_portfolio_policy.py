from __future__ import annotations

import argparse
import sys
from pathlib import Path

from portfolio_policy import (
    ACTIVE_POLICY_PATH,
    export_active_grouping,
    generate_active_grouping,
    get_current_holdings_from_db,
    normalize_ticker,
    save_active_grouping,
)


def _default_export_path(ticker: str | None) -> Path:
    if ticker is None:
        return ACTIVE_POLICY_PATH
    return ACTIVE_POLICY_PATH.with_name(f"active_policy_{ticker}.json")


def _filter_holdings_by_ticker(holdings: list[dict], ticker: str) -> list[dict]:
    normalized = normalize_ticker(ticker)
    return [
        holding
        for holding in holdings
        if normalize_ticker(holding.get("ticker")) == normalized
    ]


def run_policy_grouping(
    *,
    ticker: str | None = None,
    db_path: str | None = None,
    export_path: str | Path | None = None,
    holdings: list[dict] | None = None,
    con=None,
    save_to_db: bool = True,
) -> dict:
    """Generate, export, and optionally persist active policy grouping."""
    normalized_ticker = normalize_ticker(ticker) if ticker else None
    selected_holdings = holdings

    if selected_holdings is None:
        selected_holdings = get_current_holdings_from_db(con=con, db_path=db_path)

    if normalized_ticker:
        selected_holdings = _filter_holdings_by_ticker(
            selected_holdings,
            normalized_ticker,
        )
        if not selected_holdings:
            raise ValueError(f"{normalized_ticker} was not found in current holdings")

    active_grouping = generate_active_grouping(holdings=selected_holdings)
    output_path = Path(export_path) if export_path else _default_export_path(normalized_ticker)
    export_active_grouping(active_grouping, output_path)

    if save_to_db:
        save_active_grouping(
            active_grouping,
            con=con,
            db_path=db_path,
            replace_all=normalized_ticker is None,
        )

    active_grouping["export_path"] = str(output_path)
    active_grouping["saved_to_db"] = save_to_db
    active_grouping["replace_all"] = normalized_ticker is None
    return active_grouping


def _format_cell(value) -> str:
    if value is None:
        return ""
    return str(value)


def format_grouping_table(active_grouping: dict) -> str:
    """Return a compact terminal table for active grouping results."""
    columns = [
        ("ticker", "Ticker"),
        ("portfolio_group", "Group"),
        ("grouping_method", "Method"),
        ("grouping_status", "Status"),
    ]
    rows = active_grouping.get("holdings", [])

    if not rows:
        return "No holdings were found to group."

    widths = {
        key: max(len(title), *(len(_format_cell(row.get(key))) for row in rows))
        for key, title in columns
    }
    header = " | ".join(title.ljust(widths[key]) for key, title in columns)
    divider = "-+-".join("-" * widths[key] for key, _ in columns)
    body = [
        " | ".join(_format_cell(row.get(key)).ljust(widths[key]) for key, _ in columns)
        for row in rows
    ]

    lines = [header, divider, *body]
    warnings = active_grouping.get("warnings", [])
    if warnings:
        lines.append("")
        lines.append("Warnings:")
        lines.extend(f"- {warning}" for warning in warnings)

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate portfolio policy grouping and save it to DuckDB.",
    )
    parser.add_argument(
        "--ticker",
        help="Only run policy grouping for one ticker, for example AAPL.",
    )
    parser.add_argument(
        "--db-path",
        help="DuckDB path. Defaults to Database_Schema.DB_PATH.",
    )
    parser.add_argument(
        "--export-path",
        help="JSON export path. Defaults to exports/active_policy.json or per-ticker JSON.",
    )
    parser.add_argument(
        "--no-save-db",
        action="store_true",
        help="Export and print results without updating portfolio_grouping_active.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        active_grouping = run_policy_grouping(
            ticker=args.ticker,
            db_path=args.db_path,
            export_path=args.export_path,
            save_to_db=not args.no_save_db,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Policy grouping failed: {exc}", file=sys.stderr)
        return 1
    finally:
        from Database_Schema import close_connection

        close_connection()

    print(format_grouping_table(active_grouping))
    print(f"\nJSON export: {active_grouping['export_path']}")
    if active_grouping["saved_to_db"]:
        print("Database table updated: portfolio_grouping_active")
    else:
        print("Database update skipped.")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
