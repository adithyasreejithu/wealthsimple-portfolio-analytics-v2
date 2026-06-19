# Current Holdings Workflow

Show current holdings from the main terminal command. `src/main.py` is the only
supported CLI entrypoint. The default source is the statement `transactions`
table.

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings
```

Use email-derived transactions instead:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --holding-source email
```

Show one ticker:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --ticker AAPL
```

Print JSON:

```powershell
.\.wsvenv\Scripts\python.exe src/main.py --holdings --json
```
