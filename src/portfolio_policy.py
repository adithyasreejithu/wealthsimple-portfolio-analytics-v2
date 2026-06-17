"""
Policy and deterministic grouping tools for the AI Portfolio Brain.

The source of truth for portfolio group definitions lives in ref/policy_v1.json.
The source of truth for sector, industry, ETF, and asset-class rules lives in
ref/security_grouping_reference_v1.json.
"""

from __future__ import annotations

import json
from contextlib import nullcontext
from copy import deepcopy
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


ROOT_DIR = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT_DIR / "ref" / "policy_v1.json"
GROUPING_REFERENCE_PATH = ROOT_DIR / "ref" / "security_grouping_reference_v1.json"
ACTIVE_POLICY_PATH = ROOT_DIR / "exports" / "active_policy.json"

DEFAULT_POLICY_VERSION = "v1.0"
DEFAULT_GROUPING_REFERENCE_VERSION = "v1.0"
UNCLASSIFIED_GROUP = "Unclassified"


GROUP_SETTINGS = {
    "Core": {
        "target_weight": 0.70,
        "min_weight": 0.60,
        "primary_ticker": "XEQT",
    },
    "Income": {
        "target_weight": 0.15,
        "primary_ticker": "SCHD",
    },
    "Quality": {
        "target_weight": 0.00,
        "primary_ticker": None,
    },
    "Growth": {
        "target_weight": 0.15,
        "max_weight": 0.20,
        "primary_ticker": "PLTR",
    },
    "Alternatives": {
        "target_weight": 0.00,
        "primary_ticker": None,
    },
    "Cash": {
        "target_weight": 0.00,
        "primary_ticker": "Cash",
    },
}


MANUAL_GROUP_OVERRIDES = {
    "XEQT": "Core",
    "SCHD": "Income",
    "ZGLD": "Alternatives",
    "SMH": "Growth",
    "CASH": "Cash",
}


RISK_RULES = {
    "individual_stock_max_weight": 0.10,
    "individual_stock_excluded_tickers": ["XEQT", "SCHD", "CASH"],
}


DECISION_HIERARCHY = [
    "Portfolio Policy",
    "Risk Controls",
    "Portfolio Analytics",
    "Optimizer Recommendations",
]


def _load_json(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as file:
        return json.load(file)


def load_reference_policy(path: str | Path = POLICY_PATH) -> dict:
    """Load portfolio group definitions from the reference policy JSON file."""
    return _load_json(Path(path))


def load_security_grouping_reference(
    path: str | Path = GROUPING_REFERENCE_PATH,
) -> dict:
    """Load sector, industry, ETF, and asset-class grouping rules."""
    return _load_json(Path(path))


def _policy_groups(policy: dict | None = None) -> dict:
    policy = policy if policy is not None else load_reference_policy()
    return policy.get("groups", {})


def _build_buckets(policy: dict | None = None) -> dict:
    groups = _policy_groups(policy)
    buckets = {}

    for group_name, group_info in groups.items():
        settings = GROUP_SETTINGS.get(group_name, {})
        buckets[group_name] = {
            "target_weight": settings.get("target_weight", 0.0),
            "min_weight": settings.get("min_weight"),
            "max_weight": settings.get("max_weight"),
            "primary_ticker": settings.get("primary_ticker"),
            "purpose": group_info.get("objective") or group_info.get("description", ""),
            "description": group_info.get("description", ""),
        }

    return buckets


BUCKETS = _build_buckets()


def get_policy() -> dict:
    """Return a copy of the loaded policy plus runtime settings."""
    return {
        "buckets": deepcopy(BUCKETS),
        "manual_group_overrides": deepcopy(MANUAL_GROUP_OVERRIDES),
        "risk_rules": deepcopy(RISK_RULES),
        "decision_hierarchy": list(DECISION_HIERARCHY),
        "source_policy": load_reference_policy(),
    }


def normalize_ticker(ticker: str | None) -> str:
    """Normalize ticker symbols for policy lookups."""
    if ticker is None:
        return ""
    return str(ticker).upper().replace(".TO", "").strip()


def _normalize_text(value: Any) -> str:
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().lower()


def _contains_keyword(text: str, keyword: str) -> bool:
    return _normalize_text(keyword) in text


def _first_supported_group(group_bias: list[str], policy: dict) -> str | None:
    supported_groups = _policy_groups(policy)
    for group in group_bias:
        if group in supported_groups:
            return group
    return None


def _manual_group_for_ticker(ticker: str) -> str | None:
    return MANUAL_GROUP_OVERRIDES.get(normalize_ticker(ticker))


def get_bucket_for_ticker(ticker: str | None) -> str:
    """
    Return a quick ticker-only group.

    This is kept for compatibility with older analytics calls. Full grouping
    should use group_holding(), because that function can inspect metadata.
    """
    return _manual_group_for_ticker(normalize_ticker(ticker)) or UNCLASSIFIED_GROUP


def get_bucket_targets() -> dict[str, float]:
    """Return target weights by portfolio group."""
    return {
        bucket: values.get("target_weight", 0.0)
        for bucket, values in BUCKETS.items()
    }


def get_primary_ticker_for_bucket(bucket: str) -> str | None:
    """Return the preferred contribution ticker for a portfolio group."""
    return BUCKETS.get(bucket, {}).get("primary_ticker")


def get_position_limit() -> float:
    """Return the max allowed weight for individual stock positions."""
    return RISK_RULES["individual_stock_max_weight"]


def is_position_limit_exempt(ticker: str | None) -> bool:
    """Return True when a ticker should not trigger individual stock warnings."""
    normalized = normalize_ticker(ticker)
    return normalized in RISK_RULES["individual_stock_excluded_tickers"]


def get_security_metadata(holding: dict) -> dict:
    """Extract the metadata fields used by the grouping rules."""
    return {
        "ticker": normalize_ticker(holding.get("ticker")),
        "security_name": holding.get("security_name") or holding.get("company_name"),
        "security_type": holding.get("security_type"),
        "sector": holding.get("sector"),
        "industry": holding.get("industry"),
        "exchange": holding.get("exchange"),
        "asset_class": holding.get("asset_class") or holding.get("asset"),
        "etf_category": holding.get("etf_category"),
        "fund_family": holding.get("fund_family"),
    }


def group_holding(
    holding: dict,
    policy: dict | None = None,
    grouping_reference: dict | None = None,
) -> dict:
    """Assign one holding to a portfolio group using deterministic rules."""
    policy = policy if policy is not None else load_reference_policy()
    grouping_reference = (
        grouping_reference
        if grouping_reference is not None
        else load_security_grouping_reference()
    )
    metadata = get_security_metadata(holding)
    ticker = metadata["ticker"]
    warnings = []

    if not metadata["security_type"]:
        warnings.append(f"Missing security_type metadata for {ticker}")

    manual_group = _manual_group_for_ticker(ticker)
    if manual_group:
        return _grouping_result(
            metadata,
            manual_group,
            "manual_override",
            "grouped",
            policy,
            grouping_reference,
            warnings,
        )

    security_type = _normalize_text(metadata["security_type"])
    if security_type in {"cash", "money_market"} or ticker == "CASH":
        return _grouping_result(
            metadata,
            "Cash",
            "cash_rule",
            "grouped",
            policy,
            grouping_reference,
            warnings,
        )

    if "etf" in security_type or metadata.get("etf_category"):
        group, method = _group_etf(metadata, policy, grouping_reference)
    else:
        group, method = _group_stock(metadata, policy, grouping_reference, warnings)

    if group is None:
        return _grouping_result(
            metadata,
            UNCLASSIFIED_GROUP,
            method or "unresolved",
            "needs_review",
            policy,
            grouping_reference,
            warnings,
        )

    return _grouping_result(
        metadata,
        group,
        method,
        "grouped",
        policy,
        grouping_reference,
        warnings,
    )


def _group_etf(
    metadata: dict,
    policy: dict,
    grouping_reference: dict,
) -> tuple[str | None, str]:
    text = " ".join(
        _normalize_text(metadata.get(field))
        for field in (
            "ticker",
            "security_name",
            "asset_class",
            "etf_category",
            "fund_family",
            "sector",
            "industry",
        )
    )

    asset_classes = grouping_reference.get("etf_reference", {}).get("asset_classes", {})
    for asset_class, rules in asset_classes.items():
        keywords = rules.get("keywords", [])
        if any(_contains_keyword(text, keyword) for keyword in keywords):
            group = _first_supported_group(rules.get("default_group_bias", []), policy)
            if group:
                return group, f"etf_keyword:{asset_class}"

    return None, "etf_unresolved"


def _group_stock(
    metadata: dict,
    policy: dict,
    grouping_reference: dict,
    warnings: list[str],
) -> tuple[str | None, str]:
    sector = _normalize_text(metadata.get("sector"))
    industry = _normalize_text(metadata.get("industry"))
    ticker = metadata.get("ticker")

    if not sector:
        warnings.append(f"Missing sector metadata for {ticker}")
    if not industry:
        warnings.append(f"Missing industry metadata for {ticker}")

    equity_sectors = grouping_reference.get("equity_sectors", {})

    for sector_name, sector_rules in equity_sectors.items():
        sector_aliases = sector_rules.get("sector_aliases", [sector_name])
        sector_matches = any(
            _normalize_text(alias) == sector for alias in sector_aliases
        )

        for industry_group, industry_rules in sector_rules.get("industry_groups", {}).items():
            candidate_industries = list(industry_rules.get("industries", []))
            candidate_industries += industry_rules.get("sub_industries", [])
            industry_matches = any(
                _normalize_text(candidate) == industry
                for candidate in candidate_industries
            )
            if industry_matches:
                group = _first_supported_group(
                    industry_rules.get("default_group_bias", []),
                    policy,
                )
                if group:
                    return group, f"industry_match:{industry_group}"

        if sector_matches:
            group = _first_supported_group(
                sector_rules.get("default_group_bias", []),
                policy,
            )
            if group:
                return group, f"sector_match:{sector_name}"

    return None, "stock_unresolved"


def _grouping_result(
    metadata: dict,
    portfolio_group: str,
    grouping_method: str,
    grouping_status: str,
    policy: dict,
    grouping_reference: dict,
    warnings: list[str],
) -> dict:
    return {
        "ticker": metadata.get("ticker"),
        "portfolio_group": portfolio_group,
        "bucket": portfolio_group,
        "grouping_method": grouping_method,
        "grouping_status": grouping_status,
        "sector": metadata.get("sector"),
        "industry": metadata.get("industry"),
        "security_type": metadata.get("security_type"),
        "warnings": warnings,
        "source_policy_version": policy.get("version", DEFAULT_POLICY_VERSION),
        "source_grouping_reference_version": grouping_reference.get(
            "version",
            DEFAULT_GROUPING_REFERENCE_VERSION,
        ),
    }


def _connection_context(con=None, db_path: str | None = None):
    if con is not None:
        return nullcontext(con)

    from Database_Schema import get_connection

    if db_path is None:
        return get_connection()
    return get_connection(db_path)


def get_current_holdings_from_db(con=None, db_path: str | None = None) -> list[dict]:
    """Pull current holdings and metadata from the existing DuckDB schema."""
    query = """
        WITH latest_prices AS (
            SELECT ticker_id, adj_close AS current_price
            FROM (
                SELECT
                    ticker_id,
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
                SUM(CASE WHEN transaction = 'SELL' THEN quantity ELSE 0 END) AS sell_quantity
            FROM transactions
            GROUP BY ticker_id
        )
        SELECT
            t.ticker_symbol AS ticker,
            COALESCE(s.company_name, e.company_name) AS security_name,
            CASE
                WHEN e.ticker_id IS NOT NULL THEN 'ETF'
                WHEN s.ticker_id IS NOT NULL THEN 'EQUITY'
                ELSE NULL
            END AS security_type,
            s.sector,
            s.industry,
            s.exchange,
            e.asset AS etf_category,
            e.fund_family,
            COALESCE(tt.buy_quantity, 0) - COALESCE(tt.sell_quantity, 0) AS shares,
            (
                COALESCE(tt.buy_quantity, 0) - COALESCE(tt.sell_quantity, 0)
            ) * COALESCE(lp.current_price, 0) AS market_value
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

    return df.where(pd.notnull(df), None).to_dict(orient="records")


def generate_active_grouping(
    holdings: list[dict] | None = None,
    policy: dict | None = None,
    grouping_reference: dict | None = None,
    con=None,
    db_path: str | None = None,
    export_path: str | Path | None = None,
) -> dict:
    """Generate active grouping results for current holdings."""
    policy = policy if policy is not None else load_reference_policy()
    grouping_reference = (
        grouping_reference
        if grouping_reference is not None
        else load_security_grouping_reference()
    )
    holdings = (
        holdings
        if holdings is not None
        else get_current_holdings_from_db(con=con, db_path=db_path)
    )
    generated_at = datetime.now(timezone.utc).isoformat()
    grouped_holdings = []
    warnings = []

    for holding in holdings:
        grouped = group_holding(holding, policy, grouping_reference)
        grouped["generated_at"] = generated_at
        grouped_holdings.append(grouped)
        warnings.extend(grouped.get("warnings", []))

    active_grouping = {
        "generated_at": generated_at,
        "source_policy_version": policy.get("version", DEFAULT_POLICY_VERSION),
        "source_grouping_reference_version": grouping_reference.get(
            "version",
            DEFAULT_GROUPING_REFERENCE_VERSION,
        ),
        "holdings": grouped_holdings,
        "warnings": warnings,
    }

    if export_path is not None:
        export_active_grouping(active_grouping, export_path)

    return active_grouping


def export_active_grouping(
    active_grouping: dict,
    export_path: str | Path = ACTIVE_POLICY_PATH,
) -> None:
    """Write an active grouping JSON export for debugging and audit checks."""
    path = Path(export_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as file:
        json.dump(active_grouping, file, indent=2)


def _active_grouping_rows(active_grouping: dict) -> list[dict]:
    return [
        {
            "ticker": row.get("ticker"),
            "portfolio_group": row.get("portfolio_group"),
            "grouping_method": row.get("grouping_method"),
            "grouping_status": row.get("grouping_status"),
            "generated_at": active_grouping.get("generated_at"),
            "source_policy_version": active_grouping.get("source_policy_version"),
            "source_grouping_reference_version": active_grouping.get(
                "source_grouping_reference_version"
            ),
            "sector": row.get("sector"),
            "industry": row.get("industry"),
            "security_type": row.get("security_type"),
        }
        for row in active_grouping.get("holdings", [])
    ]


def ensure_active_grouping_table(con=None, db_path: str | None = None) -> None:
    """Create the runtime grouping table if it does not already exist."""
    with _connection_context(con, db_path) as active_con:
        active_con.execute(
            """
            CREATE TABLE IF NOT EXISTS portfolio_grouping_active (
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


def save_active_grouping(
    active_grouping: dict,
    con=None,
    db_path: str | None = None,
    *,
    replace_all: bool = True,
) -> None:
    """Persist runtime grouping results in DuckDB without duplicating references."""
    rows = _active_grouping_rows(active_grouping)
    df = pd.DataFrame(rows)

    with _connection_context(con, db_path) as active_con:
        ensure_active_grouping_table(con=active_con)

        if replace_all:
            active_con.execute("DELETE FROM portfolio_grouping_active;")

        if not df.empty:
            active_con.register("active_grouping_df", df)
            if not replace_all:
                # Per-ticker policy runs refresh only the incoming tickers.
                active_con.execute(
                    """
                    DELETE FROM portfolio_grouping_active
                    WHERE ticker IN (
                        SELECT ticker
                        FROM active_grouping_df
                    );
                    """
                )
            active_con.execute(
                """
                INSERT INTO portfolio_grouping_active (
                    ticker,
                    portfolio_group,
                    grouping_method,
                    grouping_status,
                    generated_at,
                    source_policy_version,
                    source_grouping_reference_version,
                    sector,
                    industry,
                    security_type
                )
                SELECT
                    ticker,
                    portfolio_group,
                    grouping_method,
                    grouping_status,
                    generated_at,
                    source_policy_version,
                    source_grouping_reference_version,
                    sector,
                    industry,
                    security_type
                FROM active_grouping_df;
                """
            )


def load_active_grouping(con=None, db_path: str | None = None) -> list[dict]:
    """Load the current runtime grouping table from DuckDB."""
    query = """
        SELECT
            ticker,
            portfolio_group,
            grouping_method,
            grouping_status,
            generated_at,
            source_policy_version,
            source_grouping_reference_version,
            sector,
            industry,
            security_type
        FROM portfolio_grouping_active
        ORDER BY ticker;
    """

    with _connection_context(con, db_path) as active_con:
        df = active_con.execute(query).fetchdf()

    return df.where(pd.notnull(df), None).to_dict(orient="records")
