import pandas as pd 
import yfinance as yf 
from Database_Commands import* 
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor


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


def get_securitie_info(tickers:list):
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


# tickers = ['PZA','L','RTX','ENB','SCHD', 'BRK','VFV','XEQT']
# data = get_securitie_info(tickers)