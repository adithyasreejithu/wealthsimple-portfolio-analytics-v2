import yfinance as yf 
from curl_cffi import requests


ticker = "AAPL"

try :
    session = requests.Session(impersonate='chrome')
    stock = yf.Ticker(ticker,session=session)
    info = stock.get_info()
    print(info)
except Exception as e:
        # print("Failed to fetch data:")  # Will need to turn this into a logging function 
        None

