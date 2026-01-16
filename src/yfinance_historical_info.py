import pandas as pd 
import yfinance as yf 
import os 
import duckdb 
from dotenv import load_dotenv
from Database_Commands import *
from curl_cffi import requests

# Steps process 
# 1. Call database and get last update for each stock 
# 2. Loop each stock (use parallel function)
# 3. push data into databse 

db = get_db_connnection()
load_dotenv()
START_DATE = os.getenv("START_DATE")


def get_history():
    date = get_last_date_stored(db)
    dates = dates.set_index("ticker_id")
    
    if dates.empty:
        tick_list = get_all_tickers()
        tick_list = tick_list.set_index("final_ticker")
        # add yfinance calling method here
        # print(tick_list)
    
def get_security_history(tickers: list): 
    try: 
        session = requests.Session(impersonate='chrome')
        historical_data = yf.download(tickers=tickers)
        
    except Exception as e: 
        print(f"Failed to fetch error: {e}")



tickers = ['PZA.TO','L','RTX','ENB','SCHD']
# data = get_securitie_info(tickers)

historical_data = yf.download(
    tickers=tickers,
    start="2017-01-01",
    end="2017-04-30",
    threads=True,
    group_by="ticker"
)

# for ticker in tickers:
#     df = historical_data[ticker].copy()
#     df["Ticker"] = ticker
#     df.columns.name = None  
#     print(df)


date = get_last_date_stored(db)
print(date)