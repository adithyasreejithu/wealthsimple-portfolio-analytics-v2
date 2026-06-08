import pandas as pd 
import json
import os
import yfinance as yf 
from curl_cffi import requests

stocks = [
    "PZA.TO",
    "VDY.TO",
    "CDZ.TO",
    "XEQT.TO",
    "VFV.TO",
    "BRK",
    "T",
    "INDA",
    "L",
    "SCHD",
    "ZEB.TO",
    "SPLG",
    "NVDA",
    "MCD",
    "RTX",
    "NOC",
    "PLTR",
    "ZEQT.TO",
    "ZGLD.TO",
    "WN.TO",
    "BN.TO",
    "ENB.TO",
    "AMZN",
    "AAPL",
    "AXON",
    "SPYM",
    "HIMS",
    "META",
    "NBIS"
]

session = requests.Session(impersonate="chrome")


# def test_case ():
#     """
#     threeYearAverageReturn
#     fiveYearAverageReturn
#     quoteType
#     shortName
#     longName
#     dividendYield
#     fullExchangeName

#     """
#     t = yf.Ticker("VDY.TO", session=session)
#     info = t.get_info()
#     print(info.get(""))
#     print(info.get("quoteType"))
#     print(info.get("fundFamily"))
#     print(info.get("longName"))
#     print(info.get("threeYearAverageReturn"))
#     print(info.get("totalAssets"))
#     print(info.get("yield")) # none
#     print(info.get("yield")) # none 

etf = []
equity = []
other  = []
data = {}

def etf_retrival(etfs: list) : 
    for ticker in etfs: 
        t = yf.Ticker(ticker, session=session)
        info = t.get_info()
        funds = t.funds_data

        hold = funds.top_holdings
        hold.reset_index(inplace=True)

        holdings_dict = (
            hold.set_index("Symbol")["Holding Percent"].to_dict()
        )

        sect = funds.sector_weightings
    
        data[ticker] = {
            "MER": 0,
            "AUM": info.get("totalAssets"),
            "Name": info.get("longName"),
            "3Y": info.get("threeYearAverageReturn"),
            "5Y": info.get("fiveYearAverageReturn"),
            "Div_yield": info.get("dividendRate"),
            "top_holdings" : holdings_dict,
            "sector_weights": sect
        }

def load_json(): 
    if os.path.exists("etf.json"):
        with open("etf.json", "r") as file: 
            data = json.load(file)
    
    else: 
        data ={}
    
    return data

def dump_json (data: dict): 
    with open("etf.json", "w") as file: 
        json.dump(data,file,indent=4)

def compare_divs(x: str, y: str):
    data = load_json()

    df = (
        pd.DataFrame.from_dict(data, orient="index")
        .reset_index()
        .rename(columns={"index": "ticker"})
    )
    
    # print(df[["ticker", "yield", "AUM"]])
    print(
        df[df["Type"]== "Dividend"][["ticker", "yield", "3Y","5Y"]]
    )
    # print(df.head())

    # for ticker, keys in data.items():
    #     if keys["Type"] == "Dividend":
    #         print(ticker)
    #         for title, values in keys.items():
    #             print(f"{title}: {values}")
            
    #         print("\n")


for ticker in stocks: 
    t = yf.Ticker(ticker, session=session)
    info = t.get_info()
    secType = info.get('quoteType')

    if secType == "ETF":
        etf.append(ticker) 
    elif secType == "EQUITY": 
        equity.append(ticker)
    else: 
        other.append(ticker)

# # Getting data 
# etf_retrival(etf)
# dump_json(data)
compare_divs("x","y")


