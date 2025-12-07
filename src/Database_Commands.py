import os 
import duckdb as dd 
from dotenv import load_dotenv

load_dotenv()
DB_PATH = os.getenv("DB_PATH")

def get_db_connnection(): 
    return dd.connect(DB_PATH)

def get_ticker_table(con):
    return con.execute(
        '''
            SELECT ticker_id, ticker_symbol 
            FROM tickers;
        '''
    ).fetchdf()

def get_last_date_stored(con): 
    return con.execute(
        '''
        SELECT 
            ticker_id,
            MAX(date) as lastDate
        FROM main.HistoricalRecords 
        GROUP BY ticker_id;
        '''
    ).fetchdf()

def get_all_tickers(con):
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