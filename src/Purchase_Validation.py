import os, re
import pandas as pd 
from datetime import datetime
from dotenv import load_dotenv
from system_logger import get_logger
from imap_tools import MailBox, AND, OR 
from Database_Upload import upload_email, update_email_date
from Database_Schema import get_connection
from Database_Commands import get_last_date_stored_email 

load_dotenv()

DEFAULT_DATE = os.getenv("START_DATE")
logger = get_logger(__name__)

"""
To do add threadpoolexecutor 

adding logging to this file 
"""

# Enabling env dataload 
load_dotenv()
GMAIL = os.getenv("GMAIL_USER")
PASS = os.getenv("GMAIL_PASS")

def parse_email_content(email:str, value: str, date) -> pd.DataFrame:
    """
    Reads email content and returns dataframe of purchase history


    """

    def extract(pattern,line):
        m = re.search(pattern, line)
        return (m.group(1).strip() if m else None)
    
    # splits email content line by line, removes empty lines, trailing and lead spaces and rejoins 
    # line = "\n".join(line.strip() for line in email.splitlines() if line.strip())
    line = "\n".join(l.strip() for l in email.splitlines() if l.strip())

    # removes * from old emails 
    line = line.replace("*","")
    line = line.replace("$","")
    line = line.replace("CA","")
    line = line.replace("US","")



    # print(line)

    try:
        time_str = extract(r"Time:\s*(.+)", line)

        row = {
            "account":    extract(r"Account:\s*(.+)", line),
            "transaction":       extract(r"Type:\s*(.+)", line) or value,
            "ticker":     extract(r"Symbol:\s*(.+)", line),
            "quantity":     extract(r"Shares:\s*(.+)", line),
            "avg_price":  extract(r"Average price:\s*(?:US\$|\$)?(.+)", line),
            "total_cost": extract(r"Total cost:\s*(?:US\$|\$)?(.+)", line),
            "date":       pd.to_datetime(extract(r"Time:\s*(.+)",      line)).date() if time_str else date,
            "debit":     extract(r"Amount:\s*\$?(\d+(?:\.\d{1,2})?)", line),  # add this
        }

        if value == "Dividend":
            row["type"] = value
            
        return pd.DataFrame([row])
    
    except Exception as e:
        logger.error("Extract text error took place as %s", e)

        return None


def email_handler(con):
    # variable to hold all transactions 
    ensemble = []

    # Getting last transaction uploaded
    last_date = get_last_date_stored_email(con)

    # Checking if value is not null
    if not last_date: 
        last_date = DEFAULT_DATE
    
    # Formatting value    
    last_date = datetime.strptime(last_date, "%Y-%m-%d").date()

    # opening mailbox with context manager 
    with MailBox("imap.gmail.com").login(GMAIL,PASS,"Inbox") as mb:
        for msg in mb.fetch(
            AND(
                OR(
                    AND(from_="support@wealthsimple.com"), # old email address 
                    AND(from_="notifications@o.wealthsimple.com") # new email address 
                ),
                date_gte= last_date
            )
        ):
            if "filled" in msg.subject: 
                ensemble.append(parse_email_content(msg.text, None, None))
            elif "dividend" in msg.subject: 
                received_date = msg.date.date()
                ensemble.append(parse_email_content(msg.text, "Dividend", received_date))


    tdy_date = datetime.today().date()

    df = pd.concat(ensemble, ignore_index=True)
    row = df.shape[0]

    update_email_date(con, tdy_date, row )

    upload_email(df, con)





  