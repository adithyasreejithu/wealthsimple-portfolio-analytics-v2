import os 
import duckdb as dd

def initialize_database(db_path="Data/WealthSimple.db"):    
    # Creating database file 
    con = dd.connect(db_path)

    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_tickers_id START 1;")

     # Tickers Table
    con.execute('''
        CREATE TABLE IF NOT EXISTS tickers (
            ticker_id BIGINT PRIMARY KEY DEFAULT nextval('seq_tickers_id'),
            ticker_symbol TEXT UNIQUE NOT NULL
        );
    ''')

    # Stocks Table  
    con.execute('''
        CREATE TABLE IF NOT EXISTS stocks (
            ticker_id BIGINT PRIMARY KEY,
            asset TEXT,
            company_name TEXT,
            exchange TEXT,
            currency TEXT,
            sector TEXT,
            industry TEXT,
            FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
        );
    ''')

    # ETF Table
    con.execute('''
        CREATE TABLE IF NOT EXISTS etf (
            ticker_id BIGINT PRIMARY KEY,
            asset TEXT,
            company_name TEXT,
            currency TEXT,
            fund_family TEXT,
            yield TEXT,
            expense_ratio TEXT,
            aum TEXT,
            nav TEXT,
            top_holdings TEXT,
            sector_weights TEXT,
            FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
        );
    ''')
    
    con.execute("CREATE SEQUENCE IF NOT EXISTS seq_trans_id START 1;")

    con.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            id BIGINT PRIMARY KEY DEFAULT nextval('seq_trans_id'),
            date TEXT NOT NULL,
            transaction TEXT NOT NULL,
            ticker_id BIGINT NOT NULL,
            quantity DECIMAL(18,6),
            execDate TEXT,
            debit DECIMAL(18,4),
            credit DECIMAL(18,4),
            fxRate DECIMAL(10,4),
            FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id),
            UNIQUE (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
        );
    ''')
    return con

def reset_database(db_path="Data/WealthSimple.db"):
    con = dd.connect(db_path)
    # Drop in dependency order
    con.execute("DROP TABLE IF EXISTS transactions;")
    con.execute("DROP TABLE IF EXISTS stocks;")
    con.execute("DROP TABLE IF EXISTS etf;")
    con.execute("DROP TABLE IF EXISTS tickers;")
    con.execute("DROP SEQUENCE IF EXISTS seq_tickers_id;")
    con.execute("DROP SEQUENCE IF EXISTS seq_trans_id;")
    print("All tables and sequences dropped.")


# reset_database()