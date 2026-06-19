# wealthsimple-portfolio-analytics-v2

A Python-based portfolio analytics learning project for Wealthsimple investment
data. The project is used to explore agentic AI workflows while building
practical investment analytics utilities around holdings, policy grouping,
portfolio reference metrics, and data ingestion.

## Project Structure

- `src/` - Python application and CLI modules
- `tests/` - Unit tests
- `docs/` - CLI and workflow documentation
- `Data/` - Local database and source data files
- `exports/` - Generated analytics exports
- `ref/` - Reference diagrams and policy data
- `notebooks/` - Exploratory notebooks and scratch analysis

## Python Environment

Run commands from the project root. Use the project virtual environment Python:

```powershell
.\.wsvenv\Scripts\python.exe
```

Run the test suite:

```powershell
.\.wsvenv\Scripts\python.exe -m unittest discover -s tests
```

## CLI Usage

The main portfolio CLI is `src/main.py`.

Show current holdings:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings
```

Run policy grouping:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --policy
```

See [docs/cli.md](docs/cli.md) for the full command reference.

## Agent Tooling

This repo is being set up with lightweight Node.js-based tooling for reusable
agent workflows.

The planned standard validation command is:

```powershell
npm run agent:check
```

That command should run the Python unit test suite through a small Node wrapper,
so agents and humans have one consistent health-check command.

## Data Safety

This project can handle personal investment data. Do not commit secrets, raw
personal exports, generated logs, local database changes, or credential files
unless they are intentionally reviewed.

This project is for analysis and learning. It should not present outputs as
financial advice.
