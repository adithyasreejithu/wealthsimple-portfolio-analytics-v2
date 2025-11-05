import pandas as pd 
from Database_Commands import* 




def upload_transactions(df):

   db = get_db_connnection()
   upload = df
   df = df[df['Symbol'].notnull()]
   db_tickers = get_ticker_table(db) # this returns a df 

   missing = df.loc[~df['Symbol'].isin(db_tickers['ticker_symbol']), 'Symbol'].unique().tolist()

   print(missing)

   if missing:
      new_tickers_df = pd.DataFrame({'ticker_symbol': missing})

      db.register("new_tickers_df", new_tickers_df)
      db.execute('''
               INSERT INTO tickers (ticker_symbol)
               SELECT ticker_symbol 
               FROM new_tickers_df
               WHERE ticker_symbol NOT IN (SELECT ticker_symbol FROM tickers);
                 ''')
   print(upload.head())

   upload.to_parquet("temp.parquet")
   db.execute("INSERT INTO tickers SELECT * FROM 'temp.parquet'")