import os 
import re

DPI = 300
PSM_MODE = "--psm 6"
CPU_WORKERS = os.cpu_count() -1
PDF_HEADINGS = ["Portfolio Cash", "Portfolio Equities", "Activity - Current period","Transactions for Future Settlement"]
TRANSACTION_TYPES = ["BUY","DIV","CONT","FPLINT","LOAN","SELL","NRT","RECALL"]
B_TYPES = ["BUY","DIV","LOAN","SELL","RECALL"]

HANDLED_TYPES = frozenset(["BUY", "DIV", "CONT", "SELL", "LOAN", "NRT", "RECALL", "FPLINT"])
COLUMNS = ["Stock_ID","Date","Type","ExecutedDate","Shares","FxRate","Value","Credit","Balance"]
SETTLEMENT = [
        "Transactions for Future Settlement",
        "To be Debited ($)","To be Credited ($)",
        "Settlement Date",
        "Debit ($) Credit ($) Balance ($)"
    ]

REQUIRED_COLS = {
    "BUY": ["date", "transaction", "ticker_id", "quantity", "execDate", "debit"],
    "SELL": ["date", "transaction", "ticker_id", "quantity", "execDate"],
    "LOAN": ["date", "transaction", "ticker_id", "quantity", "execDate"],
    "RECALL": ["date", "transaction", "ticker_id", "quantity", "execDate"],
    "DIV": ["date", "transaction", "ticker_id", "execDate", "credit"],
    "NRT": ["date", "transaction", "execDate", "debit"],
    "FPLINT": ["date", "transaction", "credit"],
}


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

# def get_pattern_two():
#     return r"""
#         ^(?P<date>\d{4}-\d{2}-\d{2})\s+
#         (?P<transaction>[A-Z]+)

#         (?:\s+None)?                                    

#         (?:\s+(?P<ticker_id>[A-Z0-9.]+)\s*-)?

#         (?:.*?
#             (?:
#                 (?:Bought|Sold)\s+(?P<quantity_buy>\d+(?:\.\d+)?)\s+shares
#                 |
#                 (?P<quantity_loan>\d+(?:\.\d+)?)\s+Shares?\s+on\s+loan
#                 |
#                 Loan\s+of\s+(?P<quantity_recall>\d+(?:\.\d+)?)\s+shares\s+terminated
#             )
#         )?

#         (?:.*?\(executed\s+at\s+(?P<execDate>\d{4}-\d{2}-\d{2})\))?
#     """

def get_pattern_two():
    return r"""
        ^(?P<date>\d{4}-\d{2}-\d{2})\s+
        (?P<transaction>[A-Z]+)

        (?:\s+None)?                                    

        (?:\s+(?P<ticker_id>[A-Z0-9.]+)\s*-)?

        (?:.*?
            (?:
                (?:Bought|Sold)\s+(?P<quantity_buy>\d+(?:\.\d+)?)\s+shares
                |
                (?P<quantity_loan>\d+(?:\.\d+)?)\s+Shares?\s+on\s+loan
                |
                Loan\s+of\s+(?P<quantity_recall>\d+(?:\.\d+)?)\s+shares\s+terminated
            )
        )?

        (?:.*?\(executed\s+at\s+(?P<execDate>\d{4}-\d{2}-\d{2})\))?
        (?:.*?record\s+date\s+of\s+(?P<record_date>\d{4}-\d{2}-\d{2}))?
        (?:.*?FX\s+Rate:\s+(?P<fx_rate>\d+(?:\.\d+)?))?
        
    """

def get_B_types():
    return B_TYPES

def get_settlement_keywords():
    return SETTLEMENT

def get_trans_req():
    return REQUIRED_COLS







