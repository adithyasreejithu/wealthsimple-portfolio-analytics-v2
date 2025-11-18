# import pandas as pd 
# import yfinance as yf 
# from curl_cffi import requests
# # from Database_Upload import upload_stock_info
# from Database_Commands import* 
# from concurrent.futures import ThreadPoolExecutor


# def get_info(ticker,session):
#     t = yf.Ticker(ticker,session=session)
#     info = t.get_info()

#     return (ticker, {
#         "company_name": info.get("longName"),
#         "exchange":info.get("fullExchangeName"),
#         "currency": info.get("financialCurrency"),
#         "sector": info.get("sector"),
#         "industry": info.get("industry")
#     })

# def populate_stocks_table(tickers:list): 
#     db = get_db_connnection()
#     db_tickers = get_ticker_table(db)

#     try: 
#         session = requests.Session(impersonate="chrome")

#         with ThreadPoolExecutor(max_workers=10) as executor: 
#             results = dict(
#                 executor.map(lambda t: get_info(t, session), tickers)
#             )
        
#         stock_df = pd.DataFrame.from_dict(results,orient='index').reset_index()
#         stock_df["ticker_id"] = stock_df["index"].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
        
#         stock_df.drop(columns=['index'])

#         db.register('stocks_info',stock_df)
#         db.execute('''
#             INSERT INTO stocks (ticker_id, company_name, exchange, currency, sector, industry)
#             SELECT ticker_id, company_name, exchange, currency, sector, industry
#             FROM stocks_info
#         ''')

#     except Exception as e:
#         print(f"Failed to fetch error: {e}")
    
# # This is the test case     
# ticker = ["NOC",'BN']
# populate_stocks_table(ticker)