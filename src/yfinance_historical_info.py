import pandas as pd 
import yfinance as yf 
import os 
from dotenv import load_dotenv
import duckdb 
from Database_Commands import * 

# Steps process 
# 1. Call database and get last update for each stock 
# 2. Loop each stock (use parallel function)
# 3. push data into databse 

db = get_db_connnection()
load_dotenv()
START_DATE = os.getenv("START_DATE")


def get_history():
    dates = get_last_date_stored(db)
    dates = dates.set_index("ticker_id")
    
    if dates.empty:
        tick_list = get_all_tickers(db)
        tick_list = tick_list.set_index("final_ticker")
        # add yfinance calling method here
    

get_history()