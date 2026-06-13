# Current Holdings CLI

Show current holdings from the terminal. The default source is the statement
`transactions` table.

```powershell
.\.wsvenv\Scripts\python.exe src/run_current_holdings.py
```

Use email-derived transactions instead:

```powershell
.\.wsvenv\Scripts\python.exe src/run_current_holdings.py --holding-source email
```

Show one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/run_current_holdings.py --ticker AAPL
```

Print JSON:

```powershell
.\.wsvenv\Scripts\python.exe src/run_current_holdings.py --json
```
