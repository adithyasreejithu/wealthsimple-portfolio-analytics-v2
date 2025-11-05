import os 
import duckdb as dd

def initialize_database(db_path="Data/WealthSimple.db"):    
    # Creating database file 
    con = dd.connect(db_path)

    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_tickers_id START 1;")

    con.execute('''
        CREATE TABLE IF NOT EXISTS tickers (
            ticker_id BIGINT PRIMARY KEY DEFAULT nextval('seq_tickers_id'),
            ticker_symbol TEXT UNIQUE NOT NULL
        );
    ''')

    # Stocks Table 
    con.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            ticker BIGINT PRIMARY KEY,
            ticker_id BIGINT NOT NULL,
            company_name TEXT,
            exchange TEXT,
            currency TEXT,
            sector TEXT,
            industry TEXT,
            FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
        );
    ''')
    
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_trans_id START 1;")

    # Stocks Table 
    con.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_trans_id'),
            date TEXT NOT NULL,
            transaction TEXT NOT NULL,
            ticker_id BIGINT NOT NLL,
            quantity REAL,
            execDate = TEXT
            debit REAL NOT NULL,
            credit REAL NOT NULL,
            fxRate REAL, 
            FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id) ON DELETE CASCADE
        );
    ''')
    return con
