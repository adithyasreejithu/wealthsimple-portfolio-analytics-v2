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
            SELECT ticker_symbol 
            FROM tickers;
        '''
    ).fetchdf()