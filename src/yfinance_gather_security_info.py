import os 
import pandas as pd 
import yfinance as yf 
from datetime import date
from dotenv import load_dotenv
from Database_Commands import get_db_connnection, get_last_date_stored, get_all_tickers
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor



# Setting up database connection
# db = get_db_connnection()
# Getting first date from the database 
load_dotenv()
START_DATE = os.getenv("START_DATE")


def get_info(ticker, session):
    """
    Return dict containing both etfs and stocks 

    Arguments: 
    ticker -- stock ticker from ThreadPoolExecutor
    session -- Chrome impersonation 
    """
    etf_dict = {}
    stock_dict = {}

    t = yf.Ticker(ticker, session=session)
    info = t.get_info()

    secType = info.get('quoteType')
    if secType in ['ECNQUOTE', None]: 
        new_t = ticker + ".TO"
        t = yf.Ticker(new_t, session=session)
        info = t.get_info()
        secType = info.get('quoteType')

        if secType in ['ECNQUOTE', None]:
            return etf_dict, stock_dict

    if secType == "EQUITY":
        stock_dict[ticker] = {
            "company_name": info.get("longName"),
            "asset": secType,
            "exchange": info.get("fullExchangeName"),
            "currency": info.get("financialCurrency") or info.get("currency"),
            "sector": info.get("sector"),
            "industry": info.get("industry")
        }

    elif secType == "ETF":
        funds = t.funds_data
        etf_dict[ticker] = {
            "company_name": info.get("longName"),
            "currency": info.get("financialCurrency") or info.get("currency"),
            "fund_family": info.get("fundFamily"),
            "asset": info.get("category") or info.get("fundCategory"),
            "yield": info.get("yield"),
            "expense_ratio": info.get("annualReportExpenseRatio"),
            "aum": info.get("totalAssets"),
            "nav": info.get("navPrice"),
            "top_holdings" : funds.top_holdings,
            "sector_weights": funds.sector_weightings
        }

    else:
        print(f"Unknown security type for {ticker}: {secType}")

    return etf_dict, stock_dict


def get_security_info(tickers:list):
    try: 
        session = requests.Session(impersonate='chrome')

        with ThreadPoolExecutor(max_workers=10) as executor: 
            results = list(
                executor.map(lambda t: get_info(t, session), tickers)
            )

        etfs = {k: v for pair in results for k, v in pair[0].items()}
        stocks = {k: v for pair in results for k, v in pair[1].items()}

        return etfs,stocks
   
    except Exception as e:
        print(f"Failed to fetch error: {e}")

def get_security_history():

    #temp list 
    dfs = []

    # retrieving yf dates 
    today = date.today()
    tickers_df = get_all_tickers()
    tickers = tickers_df["final_ticker"].dropna().unique().tolist()
    last_date = get_last_date_stored()

    # start date logic 
    if last_date is None or pd.isna(last_date):
        start_date = os.getenv("START_DATE")
    else: 
        start_date = last_date

    historical_data = yf.download(
        tickers=tickers,
        start=start_date,
        end=today,
        threads=True,
        auto_adjust=False,
        group_by="ticker"
    )

    for ticker in tickers: 
        df = historical_data[ticker].copy()
        df.reset_index(inplace=True)
        df["Ticker"] = ticker
        df.columns.name = None
        dfs.append(df)

    combined_df = pd.concat(dfs, ignore_index=True)
    
    return combined_df


# def get_security_history(tickers: list):

#     dfs = []

#     last_date = get_last_date_stored(db)
#     today = date.today()

#     if last_date is None or pd.isna(last_date):
#         start_date = os.getenv("START_DATE")
#     else:
#         start_date = last_date

#     historical_data = yf.download(
#         tickers=tickers,
#         start=start_date,
#         end=today,
#         threads=True,
#         group_by="ticker",
#         auto_adjust=False
#     )

#     for ticker in tickers:
#         df = historical_data[ticker].copy()
#         df.reset_index(inplace=True)
#         df["Ticker"] = ticker
#         df.columns.name = None
#         dfs.append(df)

#     combined_df = pd.concat(dfs, ignore_index=True)

#     # combined_df = combined_df.rename(columns={
        # "Date": "date",
        # "Ticker": "ticker_id",
        # "Adj Close": "adj_close",
        # "High": "high",
        # "Close": "close",
        # "Open": "open",
        # "Volume": "volume"
#     # })

#     # combined_df = combined_df.drop_duplicates(
#     #     subset=["ticker_id", "date"],
#     #     keep="last"
#     # )

#     return combined_df

# if __name__ == "__main__":
#     stonks = ['PZA.TO','L','RTX','ENB','SCHD']
#     df = get_security_history(stonks)
