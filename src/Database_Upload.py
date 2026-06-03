import json
import pandas as pd 
import yfinance as yf 
from Database_Commands import* 
from Config import get_cash_transaction_types, get_security_transaction_types
from curl_cffi import requests
from concurrent.futures import ThreadPoolExecutor
from system_logger import get_logger
from yfinance_gather_security_info import get_security_info

logger = get_logger(__name__)

# def upload_yfinance_info(etfs, stocks, db):
#    """
#    Uploads etf and stock DataFrame 
   
#    Arguments 
#    etfs -- df holding etf information
#    stocks -- DataFrame holding etf information
#    """
#    # db = get_db_connnection()
#    db_tickers = get_ticker_table(db) 

#    etfs_rows = []
#    stocks_rows = []

#    for ticker, data in etfs.items():
#       row = {"ticker":ticker}
#       row.update(data)
#       etfs_rows.append(row)
        
#    etfs_df = pd.DataFrame(etfs_rows)


#    if not etfs_df.empty:
#       etfs_df['ticker_id'] = etfs_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
#       etfs_df.drop("ticker",axis=1,inplace=True)

#       # Convert to proper JSON before uploading
#       etfs_df['top_holdings'] = etfs_df['top_holdings'].apply(
#          lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
#       )
#       etfs_df['sector_weights'] = etfs_df['sector_weights'].apply(
#          lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
#       )
   
#    for ticker, data in stocks.items():
#       row = {"ticker": ticker}
#       row.update(data)
#       stocks_rows.append(row)
        
#    stocks_df = pd.DataFrame(stocks_rows)
   
#    if not stocks_df.empty:
#       stocks_df['ticker_id'] = stocks_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
#       stocks_df.drop("ticker",axis=1,inplace=True)
   
#    if not stocks_df.empty:
#       try: 
#          db.register("stocks_df", stocks_df)
#          db.execute("""
#             INSERT OR IGNORE INTO stocks 
#                (ticker_id, asset, company_name, exchange, currency, sector, industry)
#             SELECT ticker_id, asset, company_name, exchange, currency, sector, industry
#             FROM stocks_df;
#          """)
#       except Exception as e: 
#          logger.error(f"Error uploading stock_df {e}")
   
#    if not etfs_df.empty:
#       try: 
#          db.register("etfs_df", etfs_df)
#          db.execute("""
#             INSERT OR IGNORE INTO etf 
#                (ticker_id, asset, company_name, currency, fund_family, 
#                yield, expense_ratio, aum, nav, top_holdings, sector_weights)
#             SELECT ticker_id, asset, company_name, currency, fund_family, 
#                yield, expense_ratio, aum, nav, top_holdings, sector_weights
#                FROM etfs_df;
#          """)
#       except Exception as e:
#          logger.error(f"Error uploading etfs_df {e}")


# def upload_transactions(df, db):
#    """
#    Function that deals with handling database uploads

#    df: pd.DataFrame - transaction/historical data
#    db: Database connection

#    """
   
#    # if dataframe is empty 
#    if df.empty:
#       logger.info("No transactions to upload.")
#       return
   
#    logger.info("%s transactions to be uploaded",df.shape[0])
   
#    # 
#    df = df[df['Symbol'].notnull()].copy()
#    db_tickers = get_ticker_table(db) # this returns a df 

#    # creating a list of of tickers not in the database 
#    missing = df.loc[~df['Symbol'].isin(db_tickers['ticker_symbol']), 'Symbol'].unique().tolist()
#    etfs, stocks  = get_security_info(missing)

#    # adding the missing tickers into the database 
#    if missing:
#       new_tickers_df = pd.DataFrame({'ticker_symbol': missing})

#       db.register("new_tickers_df", new_tickers_df)
#       db.execute('''
#                INSERT OR IGNORE INTO tickers (ticker_symbol)
#                SELECT ticker_symbol 
#                FROM new_tickers_df
#                WHERE ticker_symbol NOT IN (SELECT ticker_symbol FROM tickers);
#                  ''')
  
#    # matching the tickers to their database id 
#    db_tickers = get_ticker_table(db)  # needs to get new values
#    df['ticker_id'] = df['Symbol'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
#    df = df.rename(columns={
#       'Date': 'date',
#       'Type': 'transaction',
#       'Quantity': 'quantity',
#       'ExecDate': 'execDate',
#       'Debit': 'debit',
#       'Credit': 'credit',
#       'FXRate': 'fxRate'
#    })

#    upload_yfinance_info(etfs,stocks,db) 


#    # reordering the dataframe values to upload into db 
#    df = df[['date', 'transaction', 'ticker_id', 'quantity', 'execDate', 'debit', 'credit', 'fxRate']]

#    # Cleaning up data before loading into database
#    df['date'] = pd.to_datetime(df['date'], errors='coerce')
#    df['execDate'] = pd.to_datetime(df['execDate'], errors='coerce')

#    df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').round(6)
#    df['debit']    = pd.to_numeric(df['debit'], errors='coerce').round(2)
#    df['credit']   = pd.to_numeric(df['credit'], errors='coerce').round(2)
#    df['fxRate']   = pd.to_numeric(df['fxRate'], errors='coerce').round(4)

#    # filling in missing values to clean data
#    df = df.fillna({
#       'quantity': 0,
#       # 'execDate': None,
#       'debit': 0,
#       'credit': 0,
#       'fxRate': 0
#    })

#    # adding transaction data to the db
#    db.register("transactions_df", df)
#    db.execute("""
#         INSERT OR IGNORE INTO transactions (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
#         SELECT date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate
#         FROM transactions_df;
#    """)

def upload_yfinance_info(etfs, stocks, db):
   """
   Uploads etf and stock DataFrame 
   
   Arguments 
   etfs -- df holding etf information
   stocks -- DataFrame holding etf information
   """
   logger.info("Starting upload_yfinance_info | ETFs: %d, Stocks: %d", len(etfs), len(stocks))

   # db = get_db_connnection()
   db_tickers = get_ticker_table(db) 
   logger.debug("Fetched ticker table with %d rows", len(db_tickers))

   etfs_rows = []
   stocks_rows = []

   for ticker, data in etfs.items():
      row = {"ticker":ticker}
      row.update(data)
      etfs_rows.append(row)
        
   etfs_df = pd.DataFrame(etfs_rows)

   if not etfs_df.empty:
      logger.info("Processing %d ETF rows", len(etfs_df))
      etfs_df['ticker_id'] = etfs_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      etfs_df.drop("ticker",axis=1,inplace=True)

      # Convert to proper JSON before uploading
      etfs_df['top_holdings'] = etfs_df['top_holdings'].apply(
         lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
      )
      etfs_df['sector_weights'] = etfs_df['sector_weights'].apply(
         lambda x: x.to_json(orient="records") if isinstance(x, pd.DataFrame) else json.dumps(x) if x is not None else None
      )
      logger.debug("ETF JSON serialization complete")
   else:
      logger.info("No ETF rows to process, skipping")
   
   for ticker, data in stocks.items():
      row = {"ticker": ticker}
      row.update(data)
      stocks_rows.append(row)
        
   stocks_df = pd.DataFrame(stocks_rows)
   
   if not stocks_df.empty:
      logger.info("Processing %d stock rows", len(stocks_df))
      stocks_df['ticker_id'] = stocks_df['ticker'].map(db_tickers.set_index('ticker_symbol')['ticker_id'])
      stocks_df.drop("ticker",axis=1,inplace=True)
   else:
      logger.info("No stock rows to process, skipping")
   
   if not stocks_df.empty:
      try: 
         db.register("stocks_df", stocks_df)
         db.execute("""
            INSERT OR IGNORE INTO stocks 
               (ticker_id, asset, company_name, exchange, currency, sector, industry)
            SELECT ticker_id, asset, company_name, exchange, currency, sector, industry
            FROM stocks_df;
         """)
         logger.info("Successfully uploaded %d stock rows", len(stocks_df))
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
         logger.info("Successfully uploaded %d ETF rows", len(etfs_df))
      except Exception as e:
         logger.error(f"Error uploading etfs_df {e}")

   logger.info("upload_yfinance_info complete")


def _normalize_transaction_columns(df: pd.DataFrame) -> pd.DataFrame:
   df = df.copy()
   df = df.rename(columns={
      'Date': 'date',
      'Type': 'transaction',
      'Quantity': 'quantity',
      'ExecDate': 'execDate',
      'Debit': 'debit',
      'Credit': 'credit',
      'FXRate': 'fxRate'
   })

   if 'ticker_symbol' not in df.columns and 'ticker_id' in df.columns:
      df = df.rename(columns={'ticker_id': 'ticker_symbol'})

   if 'fxRate' not in df.columns and 'fx_rate' in df.columns:
      df['fxRate'] = df['fx_rate']

   for col in ['date', 'transaction', 'ticker_symbol', 'quantity', 'execDate',
               'debit', 'credit', 'fxRate', 'balance']:
      if col not in df.columns:
         df[col] = None

   df['transaction'] = df['transaction'].astype('string').str.strip().str.upper()
   return df


def _clean_common_transaction_values(df: pd.DataFrame) -> pd.DataFrame:
   df = df.copy()
   df['date'] = pd.to_datetime(df['date'], errors='coerce')
   df['execDate'] = pd.to_datetime(df['execDate'], errors='coerce')
   df['debit'] = pd.to_numeric(df['debit'], errors='coerce').fillna(0).round(2)
   df['credit'] = pd.to_numeric(df['credit'], errors='coerce').fillna(0).round(2)
   df['fxRate'] = pd.to_numeric(df['fxRate'], errors='coerce').fillna(0).round(4)
   return df


def _upload_security_transactions(df: pd.DataFrame, db):
   if df.empty:
      logger.info("No security transactions to upload.")
      return 0, 0

   df = df[df['ticker_symbol'].notnull()].copy()
   if df.empty:
      logger.warning("No security transactions had ticker symbols after filtering.")
      return 0, 0

   db_tickers = get_ticker_table(db)
   logger.debug("Fetched ticker table with %d existing tickers", len(db_tickers))

   missing = df.loc[
      ~df['ticker_symbol'].isin(db_tickers['ticker_symbol']),
      'ticker_symbol'
   ].unique().tolist()
   logger.info("%d new tickers not in DB: %s", len(missing), missing)

   etfs, stocks = get_security_info(missing)
   logger.info("Retrieved security info | ETFs: %d, Stocks: %d", len(etfs), len(stocks))

   if missing:
      new_tickers_df = pd.DataFrame({'ticker_symbol': missing})

      db.register("new_tickers_df", new_tickers_df)
      db.execute('''
               INSERT OR IGNORE INTO tickers (ticker_symbol)
               SELECT ticker_symbol
               FROM new_tickers_df
               WHERE ticker_symbol NOT IN (SELECT ticker_symbol FROM tickers);
                 ''')
      logger.info("Inserted %d new tickers into DB", len(missing))

   db_tickers = get_ticker_table(db)
   df['ticker_id'] = df['ticker_symbol'].map(
      db_tickers.set_index('ticker_symbol')['ticker_id']
   )

   upload_yfinance_info(etfs, stocks, db)

   df = _clean_common_transaction_values(df)
   df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').fillna(0).round(6)

   null_ticker_ids = df['ticker_id'].isnull().sum()
   if null_ticker_ids:
      logger.warning("%d security rows have unmapped ticker_ids and will be skipped", null_ticker_ids)
      df = df[df['ticker_id'].notnull()].copy()

   df = df[['date', 'transaction', 'ticker_id', 'quantity', 'execDate', 'debit', 'credit', 'fxRate']]
   df = df[df[['date', 'transaction', 'ticker_id']].notna().all(axis=1)].copy()

   dedupe_cols = [
      "date",
      "transaction",
      "ticker_id",
      "quantity",
      "execDate",
      "debit",
      "credit",
      "fxRate"
   ]

   before_dedupe = len(df)
   df = df.drop_duplicates(subset=dedupe_cols)
   logger.info(
      "Dropped %d duplicate security rows within dataframe before DB insert",
      before_dedupe - len(df)
   )

   before = db.execute("""
      SELECT COUNT(*) FROM transactions;
   """).fetchone()[0]

   db.register("transactions_df", df)
   db.execute("""
        INSERT INTO transactions (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
        SELECT date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate
        FROM transactions_df
        ON CONFLICT DO NOTHING;
   """)

   after = db.execute("""
      SELECT COUNT(*) FROM transactions;
   """).fetchone()[0]

   return len(df), after - before


def _upload_cash_transactions(df: pd.DataFrame, db):
   if df.empty:
      logger.info("No cash transactions to upload.")
      return 0, 0

   df = _clean_common_transaction_values(df)
   df['execDate'] = df['execDate'].fillna(df['date'])
   df['balance'] = pd.to_numeric(df['balance'], errors='coerce').round(2)
   df = df[['date', 'transaction', 'execDate', 'debit', 'credit', 'fxRate', 'balance']]
   df = df[df[['date', 'transaction']].notna().all(axis=1)].copy()

   dedupe_cols = [
      "date",
      "transaction",
      "execDate",
      "debit",
      "credit",
      "fxRate",
      "balance"
   ]

   before_dedupe = len(df)
   df = df.drop_duplicates(subset=dedupe_cols)
   logger.info(
      "Dropped %d duplicate cash rows within dataframe before DB insert",
      before_dedupe - len(df)
   )

   before = db.execute("""
      SELECT COUNT(*) FROM cash_transactions;
   """).fetchone()[0]

   db.register("cash_transactions_df", df)
   db.execute("""
        INSERT INTO cash_transactions (date, transaction, execDate, debit, credit, fxRate, balance)
        SELECT date, transaction, execDate, debit, credit, fxRate, balance
        FROM cash_transactions_df
        ON CONFLICT DO NOTHING;
   """)

   after = db.execute("""
      SELECT COUNT(*) FROM cash_transactions;
   """).fetchone()[0]

   return len(df), after - before


def upload_transactions(df, db):
   """
   Uploads extracted monthly activity into security and cash transaction tables.

   df: pd.DataFrame - transaction/historical data
   db: Database connection
   """
   if df is None:
      logger.info("No transactions to upload.")
      return

   if df.empty:
      logger.info("No transactions to upload.")
      return

   logger.info("Starting upload_transactions | Shape: %s", df.shape)

   df = _normalize_transaction_columns(df)
   security_types = get_security_transaction_types()
   cash_types = get_cash_transaction_types()
   known_types = security_types | cash_types

   unknown_df = df.loc[~df['transaction'].isin(known_types)].copy()
   if not unknown_df.empty:
      logger.warning(
         "Skipping %d transaction rows with unsupported types: %s",
         len(unknown_df),
         unknown_df['transaction'].dropna().unique().tolist()
      )

   security_df = df.loc[df['transaction'].isin(security_types)].copy()
   cash_df = df.loc[df['transaction'].isin(cash_types)].copy()

   security_submitted, security_inserted = _upload_security_transactions(security_df, db)
   cash_submitted, cash_inserted = _upload_cash_transactions(cash_df, db)

   logger.info(
      "upload_transactions complete | security_submitted=%d | security_inserted=%d | "
      "security_ignored_duplicates=%d | cash_submitted=%d | cash_inserted=%d | "
      "cash_ignored_duplicates=%d",
      security_submitted,
      security_inserted,
      security_submitted - security_inserted,
      cash_submitted,
      cash_inserted,
      cash_submitted - cash_inserted
   )

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
