# Portfolio Metrics CLI

This guide shows how to calculate portfolio metrics from the terminal. Metrics
read from DuckDB and from `portfolio_grouping_active` when that policy table is
available.

## Run Metrics Directly

Portfolio summary:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py
```

Metrics for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py --ticker AAPL
```

Use a specific database:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py --db-path Data/prd_wealthsimple.db
```

Include a contribution amount for recommendations:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py --contribution-amount 100
```

Print metrics without writing JSON:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py --no-export
```

Use email-derived transactions instead of statement transactions:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_metrics.py --holding-source email
```

## Metrics Included

Portfolio summary metrics include:

- Portfolio value, including current holdings and available cash.
- Weights by ticker, with each holding's market value and portfolio weight.
- Weights by bucket, including policy buckets and cash when available.
- Allocation drift by bucket, comparing current weight to policy target weight.
- Unrealized gains by ticker, including dollar gain and gain percentage.
- Risk metrics: total return, annualized volatility, daily volatility, max drawdown, and Sharpe ratio.
- Policy warnings for bucket limits and individual position limits.
- Recommended contribution allocation when `--contribution-amount` is provided.

Single-ticker metrics from `--ticker` include:

- Position details, including bucket, grouping method, grouping status, quantity, current price, market value, and cost basis.
- Portfolio weight for the selected ticker.
- Unrealized dollar gain and unrealized gain percentage.
- Position-limit warnings for the selected ticker.

## Run Through Main

Run metrics only, using current database data:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics
```

Run metrics only for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --ticker AAPL
```

Run metrics from email-derived transactions:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --holding-source email
```

Run the data pipeline first, then calculate metrics:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-metrics
```

Run the data pipeline first, then calculate metrics for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-metrics --ticker AAPL
```

## Output Files

Portfolio summary exports to:

```text
exports/portfolio_metrics_summary.json
```

Single-ticker metrics export to a ticker-specific file:

```text
exports/portfolio_metrics_AAPL.json
```

Pass `--export-path` to override either default.
