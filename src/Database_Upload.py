import pandas as pd 
from Database_Commands import* 

def upload_transactions(df):

   print(df)

   db = get_db_connnection()
   # upload = df
   df = df[df['Symbol'].notnull()].copy()
   db_tickers = get_ticker_table(db) # this returns a df 

   missing = df.loc[~df['Symbol'].isin(db_tickers['ticker_symbol']), 'Symbol'].unique().tolist()

   if missing:
      new_tickers_df = pd.DataFrame({'ticker_symbol': missing})

      db.register("new_tickers_df", new_tickers_df)
      db.execute('''
               INSERT INTO tickers (ticker_symbol)
               SELECT ticker_symbol 
               FROM new_tickers_df
               WHERE ticker_symbol NOT IN (SELECT ticker_symbol FROM tickers);
                 ''')
  

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

   df = df[['date', 'transaction', 'ticker_id', 'quantity', 'execDate', 'debit', 'credit', 'fxRate']]

   # Cleaning up data before loading into database
   df['quantity'] = pd.to_numeric(df['quantity'], errors='coerce').round(6)
   df['debit']    = pd.to_numeric(df['debit'], errors='coerce').round(2)
   df['credit']   = pd.to_numeric(df['credit'], errors='coerce').round(2)
   df['fxRate']   = pd.to_numeric(df['fxRate'], errors='coerce').round(4)

   df = df.fillna({
      'quantity': 0,
      'execDate': '',
      'debit': 0,
      'credit': 0,
      'fxRate': 0
   })


   db.register("transactions_df", df)
   db.execute("""
        INSERT INTO transactions (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
        SELECT date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate
        FROM transactions_df;
   """)

if __name__ == "__main__":
   test = pd.DataFrame()
   upload_transactions(test)