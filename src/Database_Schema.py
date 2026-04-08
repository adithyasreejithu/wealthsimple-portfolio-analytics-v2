import os
import duckdb as dd
from pathlib import Path
from contextlib import contextmanager
from system_logger import get_logger
from typing import Generator, Final

# DB_PATH: Final[str] = os.getenv("DB_PATH_TEST", "Data/WealthSimple_backup.db")
DB_PATH: Final[str] = os.getenv("DB_PATH", "Data/WealthSimpleProj.db")


logger = get_logger(__name__)

# Single shared connection — created once, reused everywhere
_connection: dd.DuckDBPyConnection | None = None


def get_shared_connection(db_path: str = DB_PATH) -> dd.DuckDBPyConnection:
    """
    Returns the single shared connection, creating it if needed.
    DuckDB only allows one writer this ensures we never open two at once.

    returns 
    _connection: DuckDBPyConnection

    """
    global _connection
    if _connection is None:
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        _connection = dd.connect(str(db_path))
        logger.info("Database connection opened: %s", db_path)
    return _connection


def close_connection() -> None:
    """Explicitly close the shared connection. Call this when your app shuts down."""
    global _connection
    if _connection is not None:
        _connection.close()
        _connection = None
        logger.info("Database connection closed")


@contextmanager
def get_connection(db_path: str = DB_PATH) -> Generator[dd.DuckDBPyConnection, None, None]:
    """
    Context manager for scoped database work.
    Reuses the shared connection rather than opening a new one.

    returns:
    con: DuckDBPyConnection
    
    """
    con = get_shared_connection(db_path)
    try:
        yield con # gives connection back to the function caller 
    except Exception:
        logger.exception("Error during database operation")
        raise


def _deploy_schema(con: dd.DuckDBPyConnection) -> None:
    """
    Creates all tables and sequences. 
    Expects an open connection.
    
    """
    # starts the transaction
    con.execute("BEGIN") # starting process to either commmit or roll back values 
    try:
        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_tickers_id START 1;")

        con.execute('''
            CREATE TABLE IF NOT EXISTS tickers (
                ticker_id     BIGINT PRIMARY KEY DEFAULT nextval('seq_tickers_id'),
                ticker_symbol TEXT UNIQUE NOT NULL,
                pull_date     DATE
            );
        ''')

        con.execute('''
            CREATE TABLE IF NOT EXISTS stocks (
                ticker_id    BIGINT PRIMARY KEY,
                asset        TEXT,
                company_name TEXT,
                exchange     TEXT,
                currency     VARCHAR(10),
                sector       TEXT,
                industry     TEXT,
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
            );
        ''')

        con.execute('''
            CREATE TABLE IF NOT EXISTS etf (
                ticker_id      BIGINT PRIMARY KEY,
                asset          TEXT,
                company_name   TEXT,
                currency       VARCHAR(10),
                fund_family    TEXT,
                yield          DECIMAL(10,4),
                expense_ratio  DECIMAL(10,4),
                aum            DECIMAL(20,2),
                nav            DECIMAL(18,4),
                top_holdings   JSON,
                sector_weights JSON,
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
            );
        ''')

        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_trans_id START 1;")

        con.execute('''
            CREATE TABLE IF NOT EXISTS transactions (
                id          BIGINT PRIMARY KEY DEFAULT nextval('seq_trans_id'),
                date        DATE NOT NULL,
                transaction TEXT NOT NULL,
                ticker_id   BIGINT NOT NULL,
                quantity    DECIMAL(18,6),
                execDate    DATE,
                debit       DECIMAL(18,4),
                credit      DECIMAL(18,4),
                fxRate      DECIMAL(10,6),
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id),
                UNIQUE (date, transaction, ticker_id, quantity, execDate, debit, credit, fxRate)
            );
        ''')

        con.execute('''
            CREATE TABLE IF NOT EXISTS HistoricalRecords (
                ticker_id BIGINT NOT NULL,
                date      DATE   NOT NULL,
                adj_close DOUBLE NOT NULL,
                open      DOUBLE NOT NULL,
                high      DOUBLE NOT NULL,
                low       DOUBLE NOT NULL,
                close     DOUBLE NOT NULL,
                volume    BIGINT NOT NULL,
                UNIQUE (date, ticker_id),
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id)
            );
        ''')

        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_email_store_id START 1;")

        con.execute('''
            CREATE TABLE IF NOT EXISTS EmailCheckDate (
                id          BIGINT PRIMARY KEY DEFAULT nextval('seq_email_store_id'),
                date       DATE NOT NULL,
                num_emails INT  NOT NULL
            );
        ''')

        con.execute("CREATE SEQUENCE IF NOT EXISTS seq_email_trans_id START 1;")

        con.execute('''
            CREATE TABLE IF NOT EXISTS Email_Transactions (
                id          BIGINT PRIMARY KEY DEFAULT nextval('seq_email_trans_id'),
                account     TEXT,
                transaction TEXT,
                ticker_id   BIGINT,
                ticker      TEXT,
                quantity    DECIMAL(18,6),
                avg_price   DECIMAL(18,4),
                total_cost  DECIMAL(18,4),
                debit  DECIMAL(18,4),
                date        DATE,
                FOREIGN KEY (ticker_id) REFERENCES tickers(ticker_id),
                UNIQUE (transaction, ticker_id, quantity, avg_price, total_cost, date)
            );
        ''')

        # if transaction upload is successful commit the changes to the db
        con.execute("COMMIT")
        logger.info("Schema creation committed successfully")

    except Exception:
        # an error occured therefore rollback
        con.execute("ROLLBACK")
        logger.exception("Schema creation failed, rolling back")
        raise


def initialize_database(db_path: str = DB_PATH) -> None:
    """
    Initializes all tables if they do not exist.

    returns: 
    No values
    
    """
    logger.info("Initializing database schema at %s", db_path)
    with get_connection(db_path) as con:
        _deploy_schema(con)
    logger.info("Database initialization complete")


def reset_database(db_path: str = DB_PATH, *, confirm: bool = False) -> None:
    """Drops all tables and sequences. Requires explicit confirm=True."""
    if not confirm:
        raise RuntimeError("Pass confirm=True to reset. This is irreversible.")
    logger.warning("Resetting database at %s", db_path)
    with get_connection(db_path) as con:
        for table in ("EmailTransactions", "EmailCheckDate", "transactions",
                      "stocks", "etf", "tickers", "HistoricalRecords"):
            con.execute(f"DROP TABLE IF EXISTS {table};")
        for seq in ("seq_tickers_id", "seq_trans_id", "seq_email_trans_id"):
            con.execute(f"DROP SEQUENCE IF EXISTS {seq};")
    logger.info("Database reset complete")


# reset_database(DB_PATH, confirm=True)