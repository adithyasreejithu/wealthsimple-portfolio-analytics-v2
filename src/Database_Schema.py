import duckdb as dd

# Creating database file 
con = dd.connect(r"Data\WealthSimple.db")

con.execute('''
            CREATE TABLE tickers (
                ticker_id INTEGER PRIMARY KEY,
                ticker_symbol TEXT UNIQUE NOT NULL
            );
''')


# Stocks Table 
con.execute('''
            CREATE TABLE stocks (
                stock_id INTEGER PRIMARY KEY,
                ticker_id INTEGER NOT NULL,
                company_name TEXT,
                exchange TEXT,
                currency TEXT,
                sector TEXT,
                industry TEXT,
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
            ); 
        ''')