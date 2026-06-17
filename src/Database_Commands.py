import os 
import pandas as pd
import duckdb as dd 
from dotenv import load_dotenv
from system_logger import get_logger

load_dotenv()
logger = get_logger(__name__)
# DB_PATH = os.getenv("DB_PATH_TEST")

# def get_db_connnection(): 
    # return dd.connect(DB_PATH)

def get_ticker_table(con):
    # con = get_db_connnection()
    df = con.execute(
        '''
            SELECT ticker_id, ticker_symbol 
            FROM tickers;
        '''
    ).fetchdf()
    logger.debug("Fetched ticker table | rows=%d", len(df))
    return df

def get_last_date_stored(con):
    # con = get_db_connnection()
    df = con.execute(
        '''
        SELECT MAX(date) AS lastDate
        FROM main.HistoricalRecords;
        '''
    ).fetchdf()

    result = df["lastDate"].iloc[0]

    if result is None or pd.isna(result):
        logger.info("No historical records have been stored yet")
        return None

    logger.info("Latest historical record date: %s", result.date())
    return result.date()


def get_all_tickers(con):
    # con = get_db_connnection()
    df = con.execute(
        '''
        SELECT 
            CASE 
                WHEN c.currency = 'CAD' OR e.currency = 'CAD' 
                    THEN t.ticker_symbol || '.TO'
                ELSE t.ticker_symbol
            END AS final_ticker 
        FROM main.tickers AS t
        LEFT JOIN main.stocks AS c
            ON t.ticker_id = c.ticker_id
        LEFT JOIN main.etf AS e
            ON t.ticker_id = e.ticker_id;
        '''
    ).fetchdf()
    logger.info("Fetched tickers for historical pull | rows=%d", len(df))
    return df

def get_last_date_stored_email(con):
    # con = get_db_connnection()
    df = con.execute(
        '''
        SELECT MAX(date) AS lastDate
        FROM main.Email_Transactions;
        '''
    ).fetchdf()

    result = df["lastDate"].iloc[0]

    if result is None or pd.isna(result):
        logger.info("No email transactions have been stored yet")
        return None

    logger.info("Latest email transaction date: %s", result.date())
    return result.date()
