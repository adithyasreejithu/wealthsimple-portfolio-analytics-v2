# Portfolio Policy CLI

This guide shows how to run portfolio grouping from the terminal. The command
prints a readable table, exports JSON, and updates the DuckDB table
`portfolio_grouping_active` automatically.

## Python Environment

From the project root, either activate the virtual environment:

```powershell
.\.wsvenv\Scripts\Activate.ps1
```

Or call the virtual environment Python directly:

```powershell
.\.wsvenv\Scripts\python.exe
```

## Run Policy Grouping

Run grouping for all current holdings:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_policy.py
```

Run grouping for one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_policy.py --ticker AAPL
```

Use a specific database:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_policy.py --db-path Data/prd_wealthsimple.db
```

Use a specific JSON export path:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_policy.py --export-path ref/active_policy.json
```

Print and export without updating DuckDB:

```powershell
.\.wsvenv\Scripts\python.exe src/run_portfolio_policy.py --no-save-db
```

## Run Through Main

Run the normal data pipeline only:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py
```

Run policy grouping only, using whatever data is already in the database:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy
```

Run policy grouping only for one ticker:

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

Use a specific database through `main.py`. With `--update-policy`, this path is
used for both the pipeline and policy grouping:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy --db-path Data/prd_wealthsimple.db
```

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --update-policy --db-path Data/prd_wealthsimple.db
```

## Output Files

All-holdings mode exports to:

```text
ref/active_policy.json
```

Single-ticker mode exports to a ticker-specific file by default:

```text
ref/active_policy_AAPL.json
```

Pass `--export-path` to override either default.

## Database Table

The app automatically creates and updates this table:

```sql
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
```

All-holdings mode refreshes the whole table. Single-ticker mode only replaces
that ticker's row.

Inspect all grouping results:

```sql
SELECT *
FROM portfolio_grouping_active
ORDER BY ticker;
```

Inspect one ticker:

```sql
SELECT ticker, portfolio_group, grouping_method, grouping_status
FROM portfolio_grouping_active
WHERE ticker = 'AAPL';
```

Check records that need review:

```sql
SELECT ticker, portfolio_group, grouping_method, grouping_status
FROM portfolio_grouping_active
WHERE grouping_status = 'needs_review'
ORDER BY ticker;
```
