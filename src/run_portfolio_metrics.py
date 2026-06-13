from __future__ import annotations

import argparse
import json
import sys
from contextlib import contextmanager, nullcontext
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from Database_Schema import DB_PATH, close_connection
from portfolio_metrics import (
    calculate_position_weight,
    calculate_position_weights,
    calculate_unrealized_gain_percent,
    check_position_limits,
    generate_portfolio_metrics_summary,
)
from portfolio_policy import ROOT_DIR, normalize_ticker
from portfolio_tools import get_cash_available, get_current_holdings, get_historical_values


METRICS_SUMMARY_PATH = ROOT_DIR / "ref" / "portfolio_metrics_summary.json"


@contextmanager
def _read_only_connection(con=None, db_path: str | None = None):
    if con is not None:
        with nullcontext(con) as active_con:
            yield active_con
        return

    active_con = duckdb.connect(str(db_path or DB_PATH), read_only=True)
    try:
        yield active_con
    finally:
        active_con.close()


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    return float(value)


def _default_export_path(ticker: str | None) -> Path:
    if ticker is None:
        return METRICS_SUMMARY_PATH
    return METRICS_SUMMARY_PATH.with_name(f"portfolio_metrics_{ticker}.json")


def _holdings_total(holdings: list[dict]) -> float:
    return sum(_safe_float(row.get("market_value")) for row in holdings)


def _position_for_ticker(holdings: list[dict], ticker: str) -> dict | None:
    normalized = normalize_ticker(ticker)
    for holding in holdings:
        if normalize_ticker(holding.get("ticker")) == normalized:
            return holding
    return None


def export_metrics(metrics: dict, export_path: str | Path) -> None:
    path = Path(export_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(metrics, file, indent=2, default=str)


def run_portfolio_metrics(
    *,
    ticker: str | None = None,
    db_path: str | None = None,
    export_path: str | Path | None = None,
    contribution_amount: float = 0.0,
    holdings: list[dict] | None = None,
    historical_values: list[dict] | None = None,
    cash_value: float | None = None,
    con=None,
    export: bool = True,
    holding_source: str = "transactions",
) -> dict:
    """Generate portfolio or single-position metrics from current DB data."""
    needs_db = (
        con is not None
        or holdings is None
        or cash_value is None
        or (ticker is None and historical_values is None)
    )
    connection_context = (
        _read_only_connection(con=con, db_path=db_path)
        if needs_db
        else nullcontext(None)
    )

    with connection_context as active_con:
        holdings = holdings if holdings is not None else get_current_holdings(
            con=active_con,
            source=holding_source,
        )
        if cash_value is None:
            cash_value = _safe_float(
                get_cash_available(con=active_con).get("cash_available")
            )

        normalized_ticker = normalize_ticker(ticker) if ticker else None
        if normalized_ticker:
            position = _position_for_ticker(holdings, normalized_ticker)
            if position is None:
                raise ValueError(f"{normalized_ticker} was not found in current holdings")

            portfolio_value = _holdings_total(holdings) + cash_value
            position_weights = calculate_position_weights(holdings)
            warnings = [
                warning
                for warning in check_position_limits(position_weights)
                if normalize_ticker(warning.get("ticker")) == normalized_ticker
            ]
            metrics = {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "type": "position_metrics",
                "holding_source": holding_source,
                "ticker": normalized_ticker,
                "portfolio_value": portfolio_value,
                "position": position,
                "position_weight": calculate_position_weight(
                    normalized_ticker,
                    holdings=holdings,
                    portfolio_value=portfolio_value,
                ),
                "unrealized_gain": calculate_unrealized_gain_percent(position),
                "warnings": warnings,
            }
        else:
            historical_values = (
                historical_values
                if historical_values is not None
                else get_historical_values(
                    con=active_con,
                    source=holding_source,
                )
            )
            metrics = generate_portfolio_metrics_summary(
                contribution_amount=contribution_amount,
                holdings=holdings,
                historical_values=historical_values,
                cash_value=cash_value,
            )
            metrics["generated_at"] = datetime.now(timezone.utc).isoformat()
            metrics["type"] = "portfolio_metrics_summary"
            metrics["holding_source"] = holding_source

    output_path = Path(export_path) if export_path else _default_export_path(normalized_ticker)
    if export:
        export_metrics(metrics, output_path)
    metrics["export_path"] = str(output_path)
    return metrics


def _money(value: Any) -> str:
    return f"${_safe_float(value):,.2f}"


def _percent(value: Any) -> str:
    return f"{_safe_float(value):.2%}"


def format_metrics_output(metrics: dict) -> str:
    if metrics.get("type") == "position_metrics":
        position = metrics.get("position", {})
        weight = metrics.get("position_weight", {})
        gain = metrics.get("unrealized_gain", {})
        lines = [
            f"Data source: {metrics.get('holding_source', 'transactions')}",
            f"Ticker: {metrics.get('ticker')}",
            f"Bucket: {position.get('bucket')}",
            f"Grouping: {position.get('grouping_method')} ({position.get('grouping_status')})",
            f"Quantity: {_safe_float(position.get('quantity')):,.6f}",
            f"Current price: {_money(position.get('current_price'))}",
            f"Market value: {_money(position.get('market_value'))}",
            f"Portfolio weight: {_percent(weight.get('weight'))}",
            f"Cost basis: {_money(position.get('cost_basis'))}",
            f"Unrealized gain: {_money(gain.get('unrealized_gain'))} ({_percent(gain.get('unrealized_gain_percent'))})",
        ]
    else:
        lines = [
            f"Data source: {metrics.get('holding_source', 'transactions')}",
            f"Portfolio value: {_money(metrics.get('portfolio_value'))}",
            "",
            "Bucket weights:",
        ]
        for bucket, values in metrics.get("weights_by_bucket", {}).items():
            lines.append(
                f"- {bucket}: {_percent(values.get('weight'))} ({_money(values.get('market_value'))})"
            )

        lines.extend(["", "Risk metrics:"])
        risk = metrics.get("risk_metrics", {})
        lines.append(f"- Total return: {_percent(risk.get('total_return', {}).get('total_return'))}")
        lines.append(f"- Volatility: {_percent(risk.get('volatility', {}).get('volatility'))}")
        lines.append(f"- Max drawdown: {_percent(risk.get('max_drawdown', {}).get('max_drawdown'))}")
        lines.append(f"- Sharpe ratio: {_safe_float(risk.get('sharpe_ratio', {}).get('sharpe_ratio')):.4f}")

    warnings = metrics.get("warnings", [])
    if warnings:
        lines.extend(["", "Warnings:"])
        for warning in warnings:
            lines.append(f"- {warning.get('message', warning)}")

    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate portfolio metrics from current DuckDB data.",
    )
    parser.add_argument(
        "--ticker",
        help="Only show metrics for one ticker, for example AAPL.",
    )
    parser.add_argument(
        "--db-path",
        help="DuckDB path. Defaults to Database_Schema.DB_PATH.",
    )
    parser.add_argument(
        "--export-path",
        help="JSON export path. Defaults to ref/portfolio_metrics_summary.json or per-ticker JSON.",
    )
    parser.add_argument(
        "--contribution-amount",
        type=float,
        default=0.0,
        help="Contribution amount used for portfolio contribution recommendations.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Print metrics without writing a JSON export.",
    )
    parser.add_argument(
        "--holding-source",
        choices=["transactions", "email"],
        default="transactions",
        help="Holding source for metrics. Defaults to transactions.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        metrics = run_portfolio_metrics(
            ticker=args.ticker,
            db_path=args.db_path,
            export_path=args.export_path,
            contribution_amount=args.contribution_amount,
            export=not args.no_export,
            holding_source=args.holding_source,
        )
    except ValueError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"Portfolio metrics failed: {exc}", file=sys.stderr)
        return 1
    finally:
        close_connection()

    print(format_metrics_output(metrics))
    if not args.no_export:
        print(f"\nJSON export: {metrics['export_path']}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
