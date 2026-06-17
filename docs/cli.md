# Wealthsimple Portfolio CLI

This is the master command reference for running portfolio workflows through
`src/main.py`. Run commands from the project root.

## Python Environment

Use the project virtual environment Python directly:

```powershell
.\.wsvenv\Scripts\python.exe
```

Or activate the virtual environment first:

```powershell
.\.wsvenv\Scripts\Activate.ps1
```

## Full Pipeline

Run the full data pipeline:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py
```

Use a specific DuckDB database:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --db-path Data/prd_wealthsimple.db
```

## Current Holdings

Show current holdings from existing database data:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings
```

Show one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --ticker AAPL
```

Use email-derived transactions instead of statement transactions:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --holding-source email
```

Print holdings as JSON:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --json
```

Run the full data pipeline first, then show holdings:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-holdings
```

## Policy Grouping

Run policy grouping from existing database data:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy
```

Run policy grouping for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy --ticker AAPL
```

Run the full data pipeline first, then update policy grouping:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-policy
```

Run the full data pipeline first, then update one ticker's policy grouping:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-policy --ticker AAPL
```

Use a specific JSON export path:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy --export-path exports/active_policy.json
```

## Portfolio Metrics

Run portfolio metrics from existing database data:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics
```

Run metrics for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --ticker AAPL
```

Use email-derived transactions instead of statement transactions:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --holding-source email
```

Include a contribution amount for recommendations:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --contribution-amount 100
```

Print metrics without writing JSON:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --metrics --no-export
```

Run the full data pipeline first, then calculate metrics:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-metrics
```

## What Each Command Does

`src/main.py` runs the full data pipeline. It extracts monthly statement
transactions, uploads transactions, updates stored historical prices, and
imports email transactions.

`--holdings` shows current positions from the database. It includes ticker,
source, quantity, latest stored price, market value, average cost, and policy
bucket.

`--update-holdings` runs the full data pipeline first, then shows current
holdings.

`--policy` groups current holdings into portfolio policy buckets and updates
the active policy grouping table.

`--update-policy` runs the full data pipeline first, then updates policy
grouping.

`--metrics` calculates portfolio metrics or single-ticker metrics from current
database data.

`--update-metrics` runs the full data pipeline first, then calculates metrics.

## Notes

`--holding-source email` changes which transaction table is used to calculate
quantity and cost basis. It does not change the price source.

Holdings prices come from the latest `HistoricalRecords.adj_close` value for
each ticker. Market value is calculated as quantity multiplied by that price.

Every `--update-*` command runs the full data pipeline before showing or
exporting its specific result.
