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
        r"""(?P<Date>\d{4}-\d{2}-\d{2})\s+                        # e.g. 2025-03-14
            (?P<Type>[A-Z]+)\s+                                   # BUY, CONT, DIV, FPLINT, etc.

            # Optional Symbol + Name
            (?:
                (?P<Symbol>[A-Z0-9]+)\s*[-:]\s*(?P<Name>[^:]+?):  # e.g. RTX - RTX Corporation:
            )?

            # Optional Bought/Sold and Quantity
            (?:[\s\S]*?(?:Bought|Sold)\s*(?P<Quantity>\d+(?:\.\d+)?)\s*shares)?  

            # Optional execution date
            (?:[\s\S]*?executed(?:\s*at)?(?:[\s\S]*?(?P<ExecDate>\d{4}-\d{2}-\d{2})))?  

            # "FX" may appear before the dollar amounts
            (?:[\s\S]*?FX)?                                       

            # Debit, Credit, Balance block
            (?:[\s\S]*?\$?(?P<Debit>\d+\.\d{2})\s+\$?(?P<Credit>\d+\.\d{2})\s+\$?(?P<Balance>\d+\.\d{2}))?

            # Optional "Rate:" on the next line (after FX)
            (?:[\s\S]*?Rate[:\s]*?(?P<FXRate>\d+\.\d+))?
        """,
        re.VERBOSE | re.DOTALL
    )









