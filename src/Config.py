import os 
import re

DPI = 300
PSM_MODE = "--psm 6"
CPU_WORKERS = os.cpu_count() -1
PDF_HEADINGS = ["Portfolio Cash", "Portfolio Equities", "Activity - Current period","Transactions for Future Settlement"]
TRANSACTION_TYPES = ["BUY","DIV","CONT","FPLINT","LOAN","SELL","NRT","RECALL"]
COLUMNS = ["Stock_ID","Date","Type","ExecutedDate","Shares","FxRate","Value","Credit","Balance"]

def get_transaction_dict(default_value=None):
    return {
        t_type: dict.fromkeys(COLUMNS, default_value)
        for t_type in TRANSACTION_TYPES
    }

def get_pattern():
    return re.compile(
        r"""(?P<Date>\d{4}-\d{2}-\d{2})\s+                      # 2025-04-02
            (?P<Type>[A-Z]+)\s+                                 # BUY, DIV, SELL, etc.
            (?P<Symbol>[A-Z0-9]+)\s*[-:]\s*                     # SPLG -
            (?P<Name>[^:]+?):                                   # Full stock name (until colon)
            
            # Look for quantity (before or after the $ values)
            (?:.*?(?:Bought|Sold)\s*(?P<Quantity>\d+(?:\.\d+)?)\s*shares)?  

            # Capture three $ values (Debit, Credit, Balance)
            (?:.*?\$?(?P<Debit>\d+\.\d{2})\s+\$?(?P<Credit>\d+\.\d{2})\s+\$?(?P<Balance>\d+\.\d{2}))?

            # Capture execution date (can appear anywhere)
            (?:.*?executed(?:\s*at)?(?:.*?(?P<ExecDate>\d{4}-\d{2}-\d{2})))?

            # Optional FX rate
            (?:.*?FX\s*Rate[:\s]*?(?P<FXRate>\d+\.\d+))?
        """,
        re.VERBOSE | re.DOTALL
    )
