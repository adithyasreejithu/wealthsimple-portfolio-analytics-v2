"""
Financial analytics layer for the AI Portfolio Brain.

The functions return small dictionaries that are easy for a future LLM tool
caller to consume. The calculations are intentionally simple and transparent.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd

from portfolio_policy import (
    BUCKETS,
    get_bucket_targets,
    get_position_limit,
    get_primary_ticker_for_bucket,
    is_position_limit_exempt,
    normalize_ticker,
)
from portfolio_tools import (
    get_cash_available,
    get_current_holdings,
    get_historical_values,
)


def _safe_float(value: Any, default: float = 0.0) -> float:
    if value is None or pd.isna(value):
        return default
    return float(value)


def _status_from_drift(drift: float, tolerance: float = 0.01) -> str:
    if drift < -tolerance:
        return "underweight"
    if drift > tolerance:
        return "overweight"
    return "on_target"


def _holdings_total(holdings: list[dict]) -> float:
    return sum(_safe_float(row.get("market_value")) for row in holdings)


def calculate_position_weight(
    ticker: str,
    holdings: list[dict] | None = None,
    portfolio_value: float | None = None,
) -> dict:
    """Calculate the current portfolio weight for one ticker."""
    holdings = holdings if holdings is not None else get_current_holdings()
    normalized = normalize_ticker(ticker)
    total_value = _safe_float(portfolio_value, _holdings_total(holdings))
    position = next(
        (
            row
            for row in holdings
            if normalize_ticker(row.get("ticker")) == normalized
        ),
        None,
    )

    market_value = _safe_float(position.get("market_value")) if position else 0.0
    return {
        "ticker": normalized,
        "bucket": position.get("bucket") if position else None,
        "market_value": market_value,
        "portfolio_value": total_value,
        "weight": market_value / total_value if total_value > 0 else 0.0,
    }


def calculate_position_weights(
    holdings: list[dict] | None = None,
) -> dict[str, dict]:
    """Calculate portfolio weights for all current positions."""
    holdings = holdings if holdings is not None else get_current_holdings()
    total_value = _holdings_total(holdings)

    if total_value <= 0:
        return {}

    return {
        row["ticker"]: {
            "ticker": row["ticker"],
            "bucket": row.get("bucket"),
            "market_value": _safe_float(row.get("market_value")),
            "weight": _safe_float(row.get("market_value")) / total_value,
        }
        for row in holdings
    }


def calculate_bucket_weights(
    holdings: list[dict] | None = None,
    cash_value: float = 0.0,
) -> dict[str, dict]:
    """Calculate portfolio weights by policy bucket."""
    holdings = holdings if holdings is not None else get_current_holdings()
    bucket_values: dict[str, float] = {}

    for row in holdings:
        bucket = row.get("bucket", "Unclassified")
        bucket_values[bucket] = bucket_values.get(bucket, 0.0) + _safe_float(
            row.get("market_value")
        )

    if cash_value:
        bucket_values["Cash"] = bucket_values.get("Cash", 0.0) + cash_value

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


def calculate_allocation_drift(
    bucket_weights: dict[str, dict] | None = None,
) -> dict[str, dict]:
    """Compare current bucket weights to policy targets."""
    bucket_weights = bucket_weights if bucket_weights is not None else calculate_bucket_weights()
    targets = get_bucket_targets()
    drift: dict[str, dict] = {}

    for bucket in sorted(set(targets) | set(bucket_weights)):
        current_weight = _safe_float(bucket_weights.get(bucket, {}).get("weight"))
        target_weight = _safe_float(targets.get(bucket))
        delta = current_weight - target_weight
        drift[bucket] = {
            "bucket": bucket,
            "current_weight": current_weight,
            "target_weight": target_weight,
            "drift": delta,
            "status": _status_from_drift(delta),
        }

    return drift


def calculate_unrealized_gain(position: dict) -> dict:
    """Calculate unrealized dollar gain for one position."""
    market_value = _safe_float(position.get("market_value"))
    cost_basis = _safe_float(position.get("cost_basis"))
    return {
        "ticker": position.get("ticker"),
        "bucket": position.get("bucket"),
        "market_value": market_value,
        "cost_basis": cost_basis,
        "unrealized_gain": market_value - cost_basis,
    }


def calculate_unrealized_gain_percent(position: dict) -> dict:
    """Calculate unrealized gain percent for one position."""
    gain = calculate_unrealized_gain(position)
    cost_basis = _safe_float(gain.get("cost_basis"))
    gain["unrealized_gain_percent"] = (
        _safe_float(gain.get("unrealized_gain")) / cost_basis
        if cost_basis > 0
        else 0.0
    )
    return gain


def calculate_total_return(
    historical_values: list[dict] | None = None,
) -> dict:
    """Calculate simple total return from first to last historical value."""
    historical_values = (
        historical_values if historical_values is not None else get_historical_values()
    )
    clean_values = [
        row for row in historical_values if _safe_float(row.get("portfolio_value")) > 0
    ]

    if len(clean_values) < 2:
        return {"total_return": 0.0, "start_value": 0.0, "end_value": 0.0}

    start_value = _safe_float(clean_values[0]["portfolio_value"])
    end_value = _safe_float(clean_values[-1]["portfolio_value"])
    return {
        "total_return": (end_value / start_value) - 1 if start_value > 0 else 0.0,
        "start_value": start_value,
        "end_value": end_value,
    }


def _daily_returns(historical_values: list[dict]) -> pd.Series:
    values = pd.Series(
        [_safe_float(row.get("portfolio_value")) for row in historical_values],
        dtype="float64",
    )
    values = values[values > 0]
    return values.pct_change().dropna()


def calculate_volatility(
    historical_values: list[dict] | None = None,
    periods_per_year: int = 252,
) -> dict:
    """Calculate annualized volatility from daily portfolio values."""
    historical_values = (
        historical_values if historical_values is not None else get_historical_values()
    )
    returns = _daily_returns(historical_values)

    if returns.empty:
        return {"volatility": 0.0, "daily_volatility": 0.0}

    daily_volatility = float(returns.std(ddof=0))
    return {
        "volatility": daily_volatility * math.sqrt(periods_per_year),
        "daily_volatility": daily_volatility,
    }


def calculate_max_drawdown(
    historical_values: list[dict] | None = None,
) -> dict:
    """Calculate max drawdown from daily portfolio values."""
    historical_values = (
        historical_values if historical_values is not None else get_historical_values()
    )
    values = pd.Series(
        [_safe_float(row.get("portfolio_value")) for row in historical_values],
        dtype="float64",
    )
    values = values[values > 0]

    if values.empty:
        return {"max_drawdown": 0.0}

    running_max = values.cummax()
    drawdowns = (values / running_max) - 1
    return {"max_drawdown": float(drawdowns.min())}


def calculate_sharpe_ratio(
    historical_values: list[dict] | None = None,
    risk_free_rate: float = 0.0,
    periods_per_year: int = 252,
) -> dict:
    """Calculate a basic annualized Sharpe ratio."""
    historical_values = (
        historical_values if historical_values is not None else get_historical_values()
    )
    returns = _daily_returns(historical_values)

    if returns.empty:
        return {"sharpe_ratio": 0.0}

    daily_risk_free_rate = risk_free_rate / periods_per_year
    excess_returns = returns - daily_risk_free_rate
    daily_volatility = returns.std(ddof=0)

    if daily_volatility == 0 or pd.isna(daily_volatility):
        return {"sharpe_ratio": 0.0}

    return {
        "sharpe_ratio": float(
            (excess_returns.mean() / daily_volatility) * math.sqrt(periods_per_year)
        )
    }


def check_position_limits(
    position_weights: dict[str, dict] | None = None,
) -> list[dict]:
    """Generate warnings for individual stock positions above policy limits."""
    position_weights = (
        position_weights if position_weights is not None else calculate_position_weights()
    )
    max_weight = get_position_limit()
    warnings = []

    for ticker, values in position_weights.items():
        weight = _safe_float(values.get("weight"))
        if is_position_limit_exempt(ticker):
            continue
        if weight > max_weight:
            warnings.append(
                {
                    "type": "position_limit",
                    "severity": "warning",
                    "ticker": ticker,
                    "current_weight": weight,
                    "limit": max_weight,
                    "message": (
                        f"{ticker} is {weight:.1%}, above the "
                        f"{max_weight:.1%} individual stock limit."
                    ),
                }
            )

    return warnings


def check_bucket_limits(
    bucket_weights: dict[str, dict] | None = None,
) -> list[dict]:
    """Generate warnings for policy bucket limit breaches."""
    bucket_weights = bucket_weights if bucket_weights is not None else calculate_bucket_weights()
    warnings = []
    for bucket, rules in BUCKETS.items():
        weight = _safe_float(bucket_weights.get(bucket, {}).get("weight"))
        min_weight = rules.get("min_weight")
        max_weight = rules.get("max_weight")

        if min_weight is not None and weight < _safe_float(min_weight):
            warnings.append(
                {
                    "type": "bucket_limit",
                    "severity": "warning",
                    "bucket": bucket,
                    "current_weight": weight,
                    "limit": _safe_float(min_weight),
                    "message": (
                        f"{bucket} is {weight:.1%}, below the "
                        f"{_safe_float(min_weight):.1%} minimum."
                    ),
                }
            )

        if max_weight is not None and weight > _safe_float(max_weight):
            warnings.append(
                {
                    "type": "bucket_limit",
                    "severity": "warning",
                    "bucket": bucket,
                    "current_weight": weight,
                    "limit": _safe_float(max_weight),
                    "message": (
                        f"{bucket} is {weight:.1%}, above the "
                        f"{_safe_float(max_weight):.1%} warning threshold."
                    ),
                }
            )

    return warnings


def recommend_contribution_allocation(
    amount: float,
    bucket_weights: dict[str, dict] | None = None,
) -> dict:
    """
    Recommend where new contribution dollars should go.

    The recommendation prioritizes underweight buckets and avoids adding to
    buckets that are already above target or above risk limits.
    """
    amount = _safe_float(amount)
    bucket_weights = bucket_weights if bucket_weights is not None else calculate_bucket_weights()
    current_total = sum(
        _safe_float(values.get("market_value")) for values in bucket_weights.values()
    )

    if amount <= 0:
        return {
            "amount": amount,
            "allocations": {},
            "notes": ["No contribution amount provided."],
        }

    targets = get_bucket_targets()
    current_gaps = {}
    for bucket, target in targets.items():
        if target <= 0:
            continue
        current_value = _safe_float(bucket_weights.get(bucket, {}).get("market_value"))
        current_gaps[bucket] = max((target * current_total) - current_value, 0.0)

    eligible_gaps = {
        bucket: gap
        for bucket, gap in current_gaps.items()
        if gap > 0 and not _bucket_is_above_risk_limit(bucket, bucket_weights)
    }

    allocations: dict[str, dict] = {}
    notes = []

    if eligible_gaps:
        total_gap = sum(eligible_gaps.values())
        for bucket, gap in eligible_gaps.items():
            allocation = amount * (gap / total_gap)
            allocations[bucket] = {
                "bucket": bucket,
                "ticker": get_primary_ticker_for_bucket(bucket),
                "amount": allocation,
                "reason": "Bucket is under target allocation.",
            }
    else:
        eligible_targets = {
            bucket: target
            for bucket, target in targets.items()
            if target > 0 and not _bucket_is_above_risk_limit(bucket, bucket_weights)
        }
        total_target = sum(eligible_targets.values())
        for bucket, target in eligible_targets.items():
            allocation = amount * (target / total_target) if total_target > 0 else 0.0
            allocations[bucket] = {
                "bucket": bucket,
                "ticker": get_primary_ticker_for_bucket(bucket),
                "amount": allocation,
                "reason": "No bucket is underweight; following policy target mix.",
            }
        notes.append("No underweight bucket found; allocation follows target mix.")

    for bucket in bucket_weights:
        if _bucket_is_above_risk_limit(bucket, bucket_weights):
            notes.append(f"Avoid adding to {bucket} until it falls below its risk limit.")

    return {
        "amount": amount,
        "allocations": allocations,
        "notes": notes,
    }


def _bucket_is_above_risk_limit(bucket: str, bucket_weights: dict[str, dict]) -> bool:
    max_weight = BUCKETS.get(bucket, {}).get("max_weight")
    if max_weight is None:
        return False

    weight = _safe_float(bucket_weights.get(bucket, {}).get("weight"))
    return weight > _safe_float(max_weight)


def generate_portfolio_metrics_summary(
    contribution_amount: float = 0.0,
    holdings: list[dict] | None = None,
    historical_values: list[dict] | None = None,
    cash_value: float | None = None,
) -> dict:
    """Generate the AI-readable master portfolio metrics summary."""
    holdings_were_provided = holdings is not None
    holdings = holdings if holdings is not None else get_current_holdings()
    if cash_value is None:
        cash_value = (
            0.0
            if holdings_were_provided
            else _safe_float(get_cash_available().get("cash_available"))
        )
    historical_values = (
        historical_values if historical_values is not None else get_historical_values()
    )
    portfolio_value = _holdings_total(holdings) + cash_value
    position_weights = calculate_position_weights(holdings)
    bucket_weights = calculate_bucket_weights(holdings, cash_value=cash_value)
    allocation_drift = calculate_allocation_drift(bucket_weights)
    warnings = check_position_limits(position_weights) + check_bucket_limits(bucket_weights)

    return {
        "portfolio_value": portfolio_value,
        "weights_by_ticker": position_weights,
        "weights_by_bucket": bucket_weights,
        "allocation_drift": allocation_drift,
        "unrealized_gains": {
            row["ticker"]: calculate_unrealized_gain_percent(row)
            for row in holdings
        },
        "risk_metrics": {
            "total_return": calculate_total_return(historical_values),
            "volatility": calculate_volatility(historical_values),
            "max_drawdown": calculate_max_drawdown(historical_values),
            "sharpe_ratio": calculate_sharpe_ratio(historical_values),
        },
        "warnings": warnings,
        "recommended_contribution_allocation": recommend_contribution_allocation(
            contribution_amount,
            bucket_weights,
        ),
    }
