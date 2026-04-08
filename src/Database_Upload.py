import json
import pandas as pd 
import yfinance as yf 
from Database_Commands import* 
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from system_logger import get_logger
from yfinance_gather_security_info import get_security_info

logger = get_logger(__name__)

def upload_yfinance_info(etfs, stocks, db):
   """
   Uploads etf and stock DataFrame 
   
   Arguments 
   etfs -- df holding etf information
   stocks -- DataFrame holding etf information
   """
   # db = get_db_connnection()
   db_tickers = get_ticker_table(db) 

   etfs_rows = []
   stocks_rows = []

   for ticker, data in etfs.items():
      row = {"ticker":ticker}
      row.update(data)
      etfs_rows.append(row)
        
   etfs_df = pd.DataFrame(etfs_rows)


   if not etfs_df.empty:
      etfs_df['ticker_id'] = etfs_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      etfs_df.drop("ticker",axis=1,inplace=True)

      # Convert to proper JSON before uploading
      etfs_df['top_holdings'] = etfs_df['top_holdings'].apply(
         lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
      )
      etfs_df['sector_weights'] = etfs_df['sector_weights'].apply(
         lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
      )
   
   for ticker, data in stocks.items():
      row = {"ticker": ticker}
      row.update(data)
      stocks_rows.append(row)
        
   stocks_df = pd.DataFrame(stocks_rows)
   
   if not stocks_df.empty:
      stocks_df['ticker_id'] = stocks_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      stocks_df.drop("ticker",axis=1,inplace=True)
   
   if not stocks_df.empty:
      try: 
         db.register("stocks_df", stocks_df)
         db.execute("""
            INSERT OR IGNORE INTO stocks 
               (ticker_id, asset, company_name, exchange, currency, sector, industry)
            SELECT ticker_id, asset, company_name, exchange, currency, sector, industry
            FROM stocks_df;
         """)
      except Exception as e: 
         logger.error(f"Error uploading stock_df {e}")
   
   if not etfs_df.empty:
      try: 
         db.register("etfs_df", etfs_df)
         db.execute("""
            INSERT OR IGNORE INTO etf 
               (ticker_id, asset, company_name, currency, fund_family, 
               yield, expense_ratio, aum, nav, top_holdings, sector_weights)
            SELECT ticker_id, asset, company_name, currency, fund_family, 
               yield, expense_ratio, aum, nav, top_holdings, sector_weights
               FROM etfs_df;
         """)
      except Exception as e:
         logger.error(f"Error uploading etfs_df {e}")


def upload_transactions(df, db):
   """
   Function that deals with handling database uploads

   df: pd.DataFrame - transaction/historical data
   db: Database connection

   """
   
   # if dataframe is empty 
   if df.empty:
      logger.info("No transactions to upload.")
      return
   
   logger.info("%s transactions to be uploaded",df.shape[0])
   
   # 
   df = df[df['Symbol'].notnull()].copy()
   db_tickers = get_ticker_table(db) # this returns a df 

   # creating a list of of tickers not in the database 
   missing = df.loc[~df['Symbol'].isin(db_tickers['ticker_symbol']), 'Symbol'].unique().tolist()
   etfs, stocks  = get_security_info(missing)

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

   upload_yfinance_info(etfs,stocks,db) 


   # reordering the dataframe values to upload into db 
   df = df[['date', 'transaction', 'ticker_id', 'quantity', 'execDate', 'debit', 'credit', 'fxRate']]

   # Cleaning up data before loading into database
   df['date'] = pd.to_datetime(df['date'], errors='coerce')
   df['execDate'] = pd.to_datetime(df['execDate'], errors='coerce')

   df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').round(6)
   df['debit']    = pd.to_numeric(df['debit'], errors='coerce').round(2)
   df['credit']   = pd.to_numeric(df['credit'], errors='coerce').round(2)
   df['fxRate']   = pd.to_numeric(df['fxRate'], errors='coerce').round(4)

   # filling in missing values to clean data
   df = df.fillna({
      'quantity': 0,
      # 'execDate': None,
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


def upload_history(history_df: pd.DataFrame, db):
   # db = get_db_connnection()
   tick_map = get_ticker_table(db)
   print(tick_map)

 
   history_df = history_df.rename(columns={
      "Date": "date",
      "Ticker": "ticker_symbol",
      "Adj Close": "adj_close",
      "High": "high",
      "Low": "low",
      "Close": "close",
      "Open": "open",
      "Volume": "volume"
   })

   
   history_df["ticker_symbol_base"] = history_df["ticker_symbol"].str.replace(
      r"\.TO$", "", regex=True
   )

   
   history_df["ticker_id"] = history_df["ticker_symbol_base"].map(
      tick_map.set_index("ticker_symbol")["ticker_id"]
   )

   
   history_df = history_df[
      history_df[["date","ticker_id","adj_close","high","low","close","open","volume"]]
      .notna()
      .all(axis=1)
   ].copy()

   db.register('history_df', history_df)
   db.execute("""
      INSERT OR IGNORE INTO HistoricalRecords (date, ticker_id, adj_close, high, low, close, open, volume)
      SELECT date, ticker_id, adj_close, high, low, close, open, volume
      FROM history_df;
   """)
def upload_email (emails_df : pd.DataFrame, db): 
   
   tick_map = get_ticker_table(db)
   emails_df["ticker_id"] = emails_df["ticker"].map(tick_map.set_index("ticker_symbol")["ticker_id"])
   
   db.register('email_df', emails_df)
   db.execute("""
      INSERT OR IGNORE INTO Email_Transactions(account, transaction, ticker_id, ticker, quantity, 
      avg_price, total_cost, debit, date)
      SELECT account, transaction, ticker_id, ticker, quantity, avg_price, total_cost, debit, date
      FROM email_df;
   """)

def update_email_date(con, email_Date, count):
    con.execute(
        """
        INSERT OR IGNORE INTO EmailCheckDate (date, num_emails)
        VALUES (?, ?)
        """,
        (email_Date, count)
    )
