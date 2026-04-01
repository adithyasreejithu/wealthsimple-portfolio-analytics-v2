import os, re
import pandas as pd 
from dotenv import load_dotenv
from system_logger import get_logger
from imap_tools import MailBox, AND, OR 

logger = get_logger(__name__)

"""
To do add threadpoolexecutor 

adding logging to this file 
"""

# Enabling env dataload 
load_dotenv()
GMAIL = os.getenv("GMAIL_USER")
PASS = os.getenv("GMAIL_PASS")

def parse_email_content(email:str):

    def extract(pattern,line):
        m = re.search(pattern, line)
        return (m.group(1).strip() if m else None)
    
    line = "\n".join(line.strip() for line in email.splitlines() if line.strip())
    line = line.replace("*","")


    row = {
        "account":    extract(r"Account:\s*(.+)",                  line),
        "type":       extract(r"Type:\s*(.+)",                     line),
        "symbol":     extract(r"Symbol:\s*(.+)",                   line),
        "shares":     extract(r"Shares:\s*(.+)",                   line),
        "avg_price":  extract(r"Average price:\s*(?:US\$|\$)?(.+)",line),
        "total_cost": extract(r"Total cost:\s*(?:US\$|\$)?(.+)",   line),
        "time":       extract(r"Time:\s*(.+)",                     line),
    }

    return pd.DataFrame([row])

ensemble = []
# opening mailbox with context manager 
with MailBox("imap.gmail.com").login(GMAIL,PASS,"Inbox") as mb:
    for msg in mb.fetch(
        AND(
            OR(
                AND(from_="support@wealthsimple.com"), # old email address 
                AND(from_="notifications@o.wealthsimple.com") # new email address 
            )
        ),
        limit=5,
        reverse=True
    ):
        if "filled" in msg.subject: 
            ensemble.append(parse_email_content(msg.text))
        # elif "dividend" in msg.subject: 
        #     print("later issue")

df = pd.concat(ensemble, ignore_index=True)


print(df.head(10))

