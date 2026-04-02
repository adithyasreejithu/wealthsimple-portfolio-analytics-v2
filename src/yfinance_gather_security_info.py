import os 
import pandas as pd 
import yfinance as yf 
from datetime import date
from dotenv import load_dotenv
# from Database_Commands import get_db_connnection, get_last_date_stored, get_all_tickers
from Database_Commands import  get_last_date_stored, get_all_tickers
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from system_logger import get_logger

logger = get_logger(__name__)

# Loading Env variables 
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
            # "top_holdings" : funds.top_holdings,
            "top_holdings" : getattr(funds, "top_holdings", None),
            "sector_weights": funds.sector_weightings
        }

    else:
        print(f"Unknown security type for {ticker}: {secType}")
        logger.info("Unknown security type for %s: %s", ticker, secType)

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

def get_security_history(con):
    """
    Pulls Historical Data for securities 

    Con: Database connection
    """

    #temp list 
    dfs = []

    # retrieving yf dates 
    today = date.today()
    today_pull = today - pd.Timedelta(days=1)
    tickers_df = get_all_tickers(con)
    tickers = tickers_df["final_ticker"].dropna().unique().tolist()
    last_date = get_last_date_stored(con)

    logger.info("Last date pulled: %s", last_date)

    # start date logic 
    if last_date is None or pd.isna(last_date):
        logger.info("Last pulled data is empty - Using portfolio first date")
        start_date = os.getenv("START_DATE")
    else: 
        if today_pull != last_date:
            start_date = last_date
        else : 
            logger.info("Data was pulled today - no pull was made ")
            return

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

