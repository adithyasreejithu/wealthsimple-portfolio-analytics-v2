import pandas as pd 
import yfinance as yf 
from Database_Commands import* 
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from yfinance_gather_security_info import*

def upload_yfinance_info(etfs, stocks):
   db = get_db_connnection()
   db_tickers = get_ticker_table(db) 

   etfs_rows = []
   stocks_rows = []

   for ticker, data in etfs.items():
      row = {"ticker":ticker}
      row.update(data)
      etfs_rows.append(row)
        
   etfs_df = pd.DataFrame(etfs_rows)

   print(etfs_df)

   if not etfs_df.empty:
      etfs_df['ticker_id'] = etfs_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      etfs_df.drop("ticker",axis=1,inplace=True)
   #   print(etfs_df)

   for ticker, data in stocks.items():
      row = {"ticker": ticker}
      row.update(data)
      stocks_rows.append(row)
        
   stocks_df = pd.DataFrame(stocks_rows)
   print(stocks_rows)
   
   if not stocks_df.empty:
      stocks_df['ticker_id'] = stocks_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      stocks_df.drop("ticker",axis=1,inplace=True)
   
   if not stocks_df.empty:
        db.register("stocks_df", stocks_df)
        db.execute("""
            INSERT OR IGNORE INTO stocks 
               (ticker_id, asset, company_name, exchange, currency, sector, industry)
            SELECT ticker_id, asset, company_name, exchange, currency, sector, industry
            FROM stocks_df;
        """)

    # ---- Upload ETF data IF EXISTS ----
   if not etfs_df.empty:
      db.register("etfs_df", etfs_df)
      db.execute("""
         INSERT OR IGNORE INTO etf 
            (ticker_id, asset, company_name, currency, fund_family, 
            yield, expense_ratio, aum, nav, top_holdings, sector_weights)
         SELECT ticker_id, asset, company_name, currency, fund_family, 
            yield, expense_ratio, aum, nav, top_holdings, sector_weights
            FROM etfs_df;
      """)




def upload_transactions(df):
   # Setting up database connectiond
   db = get_db_connnection()
   df = df[df['Symbol'].notnull()].copy()
   db_tickers = get_ticker_table(db) # this returns a df 

   # creating a list of of tickers not in the database 
   missing = df.loc[~df['Symbol'].isin(db_tickers['ticker_symbol']), 'Symbol'].unique().tolist()
   etfs, stocks  = get_securitie_info(missing)

   # adding the missing tickers into the database 
   if missing:
      new_tickers_df = pd.DataFrame({'ticker_symbol': missing})

      db.register("new_tickers_df", new_tickers_df)
      db.execute('''
               INSERT OR IGNORE INTO tickers (ticker_symbol)
               SELECT ticker_symbol 
               FROM new_tickers_df
               WHERE ticker_symbol NOT IN (SELECT ticker_symbol FROM tickers);
                 ''')
  
   # matching the tickers to their database id 
   db_tickers = get_ticker_table(db)  # needs to get new values
   df['ticker_id'] = df['Symbol'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
   df = df.rename(columns={
      'Date': 'date',
      'Type': 'transaction',
      'Quantity': 'quantity',
      'ExecDate': 'execDate',
      'Debit': 'debit',
      'Credit': 'credit',
      'FXRate': 'fxRate'
   })

   upload_yfinance_info(etfs,stocks) 


   # reordering the dataframe values to upload into db 
   df = df[['date', 'transaction', 'ticker_id', 'quantity', 'execDate', 'debit', 'credit', 'fxRate']]

   # Cleaning up data before loading into database
   df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').round(6)
   df['debit']    = pd.to_numeric(df['debit'], errors='coerce').round(2)
   df['credit']   = pd.to_numeric(df['credit'], errors='coerce').round(2)
   df['fxRate']   = pd.to_numeric(df['fxRate'], errors='coerce').round(4)

   # filling in missing values to clean data
   df = df.fillna({
      'quantity': 0,
      'execDate': '',
      'debit': 0,
      'credit': 0,
      'fxRate': 0
   })

   # adding transaction data to the db
   db.register("transactions_df", df)
   db.execute("""
        INSERT OR IGNORE INTO transactions (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
        SELECT date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate
        FROM transactions_df;
   """)
