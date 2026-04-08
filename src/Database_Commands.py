import os 
import pandas as pd
import duckdb as dd 
from dotenv import load_dotenv

load_dotenv()
# DB_PATH = os.getenv("DB_PATH_TEST")

# def get_db_connnection(): 
    # return dd.connect(DB_PATH)

def get_ticker_table(con):
    # con = get_db_connnection()
    return con.execute(
        '''
            SELECT ticker_id, ticker_symbol 
            FROM tickers;
        '''
    ).fetchdf()

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
        return None

    return result.date()


def get_all_tickers(con):
    # con = get_db_connnection()
    return con.execute(
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
        return None

    return result.date()
