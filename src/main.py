import argparse
import json
import sys
from Database_Schema import initialize_database, get_connection, close_connection
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline, move_read_file
from MonthlyFileExtract import camelot_extraction_pipeline
from concurrent.futures import ProcessPoolExecutor
from yfinance_gather_security_info import get_security_history
from system_logger import get_logger
from Purchase_Validation import email_handler
from FormatFiles import rename_file
from run_portfolio_metrics import format_metrics_output, run_portfolio_metrics
from run_portfolio_policy import format_grouping_table, run_policy_grouping
from run_current_holdings import (
    _filter_holdings_by_ticker,
    format_holdings_table,
    get_current_holdings,
)
from portfolio_policy import normalize_ticker
import pandas as pd

logger = get_logger(__name__)


def _log_table_counts(con, label: str) -> None:
    tables = [
        "tickers",
        "transactions",
        "cash_transactions",
        "HistoricalRecords",
        "Email_Transactions",
        "EmailCheckDate",
    ]
    counts = {}
    for table in tables:
        try:
            counts[table] = con.execute(f"SELECT COUNT(*) FROM {table};").fetchone()[0]
        except Exception as exc:
            counts[table] = f"unavailable: {exc}"
    logger.info("%s table counts: %s", label, counts)


def ocr_method(con) -> None:
    """
    Runs the OCR extraction pipeline for any new monthly report files.
    """
    rename_file()

    file_list = check_data_files()

    if not file_list:
        logger.info("No new files found")
        return

    logger.info("New file(s) found: %s", len(file_list))

    for file in file_list:
        try:
            trans_df = extraction_pipline(file)

            if trans_df is None or trans_df.empty:
                logger.warning("Empty DataFrame, file skipped: %s", file)
                continue

            upload_transactions(trans_df, con)
            move_read_file(file)

        except Exception:
            logger.error("Failed to process file: %s", file, exc_info=True)


def process_pdf(file):
    logger.info("Starting PDF extraction worker | file=%s", file)
    data = camelot_extraction_pipeline(file)
    move_read_file(file)
    logger.info(
        "Finished PDF extraction worker | file=%s | rows=%d",
        file,
        0 if data is None else len(data),
    )
    return data


def camelot_method() -> pd.DataFrame:
    """Extract new monthly statement PDFs and return one upload-ready frame."""
    logger.info("Starting Camelot monthly statement extraction")

    rename_file()

    file_list = check_data_files()

    if not file_list:
        logger.info("No new files found")
        return

    logger.info(
        "New file(s) found: %s | files=%s",
        len(file_list),
        [file.name for file in file_list],
    )

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_pdf, file_list))

    valid = [r for r in results if r is not None and not r.empty]
    logger.info(
        "Camelot worker results collected | files=%d | valid_dataframes=%d | rows=%d",
        len(results),
        len(valid),
        sum(len(df) for df in valid),
    )

    if not valid:
        logger.warning("No valid results to upload")
        return

    combined = pd.concat(valid, ignore_index=True)
    combined.to_csv("Upload.csv", index=False)
    logger.info("Saved %s rows to Upload.csv", len(combined))

    return combined


def email_method(con) -> None:
    """Run email-based transaction ingestion against the active connection."""
    logger.info("Starting email transaction ingestion")
    email_handler(con)
    logger.info("Email transaction ingestion complete")


def _connection_for_path(db_path: str | None = None):
    if db_path is None:
        return get_connection()
    return get_connection(db_path)


def run_data_pipeline(db_path: str | None = None) -> None:
    logger.info("%s", "=" * 96)
    logger.info("Data pipeline initiated | db_path=%s", db_path or "default")

    if db_path is None:
        initialize_database()
    else:
        initialize_database(db_path)

    try:
        with _connection_for_path(db_path) as con:
            _log_table_counts(con, "Before monthly statement upload")
            data = camelot_method()
            logger.info(
                "Monthly statement extraction returned | rows=%d",
                0 if data is None else len(data),
            )
            upload_transactions(data, con)
            _log_table_counts(con, "After monthly statement upload")
    except Exception:
        logger.exception("Monthly statement transaction upload failed")
    finally:
        close_connection()
        logger.info("Monthly statement transaction process completed")

    try:
        with _connection_for_path(db_path) as con:
            _log_table_counts(con, "Before historical/email upload")
            hist_data = get_security_history(con)
            logger.info(
                "Historical data pull returned | rows=%d",
                0 if hist_data is None else len(hist_data),
            )
            if hist_data is not None:
                upload_history(hist_data, con)
                _log_table_counts(con, "After historical upload")
            email_method(con)
            _log_table_counts(con, "After email upload")
    except Exception:
        logger.exception("Historical or email transaction upload failed")
    finally:
        close_connection()
        logger.info("Data pipeline completed")
        logger.info("%s", "=" * 96)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Wealthsimple portfolio data pipeline.",
    )
    parser.add_argument(
        "--holdings",
        action="store_true",
        help="Show current holdings instead of running the data pipeline.",
    )
    parser.add_argument(
        "--update-holdings",
        action="store_true",
        help="Run the data pipeline first, then show current holdings.",
    )
    parser.add_argument(
        "--policy",
        action="store_true",
        help="Run portfolio policy grouping instead of the data pipeline.",
    )
    parser.add_argument(
        "--update-policy",
        action="store_true",
        help="Run the data pipeline first, then update portfolio policy grouping.",
    )
    parser.add_argument(
        "--metrics",
        action="store_true",
        help="Run portfolio metrics instead of the data pipeline.",
    )
    parser.add_argument(
        "--update-metrics",
        action="store_true",
        help="Run the data pipeline first, then calculate portfolio metrics.",
    )
    parser.add_argument(
        "--ticker",
        help=(
            "Only run holdings, policy grouping, or metrics for one ticker. "
            "Requires a holdings, policy, or metrics flag."
        ),
    )
    parser.add_argument(
        "--db-path",
        help="DuckDB path to use for the selected command.",
    )
    parser.add_argument(
        "--export-path",
        help="JSON export path to use for policy grouping or metrics.",
    )
    parser.add_argument(
        "--contribution-amount",
        type=float,
        default=0.0,
        help="Contribution amount used for metrics contribution recommendations.",
    )
    parser.add_argument(
        "--no-export",
        action="store_true",
        help="Print metrics without writing a JSON export.",
    )
    parser.add_argument(
        "--holding-source",
        choices=["transactions", "email"],
        default="transactions",
        help="Holding source for holdings and metrics. Defaults to transactions.",
    )
    parser.add_argument(
        "--json",
        action="store_true",
        help="Print current holdings as JSON. Requires --holdings or --update-holdings.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    logger.info("CLI arguments parsed: %s", vars(args))
    mode_flags = [
        args.holdings,
        args.update_holdings,
        args.policy,
        args.update_policy,
        args.metrics,
        args.update_metrics,
    ]

    if sum(1 for enabled in mode_flags if enabled) > 1:
        parser.error(
            "Use only one of --holdings, --update-holdings, --policy, "
            "--update-policy, --metrics, or --update-metrics"
        )

    if args.ticker and not any(mode_flags):
        parser.error("--ticker requires a holdings, policy, or metrics flag")

    if args.export_path and not (
        args.policy or args.update_policy or args.metrics or args.update_metrics
    ):
        parser.error("--export-path requires --policy, --update-policy, --metrics, or --update-metrics")

    if args.contribution_amount and not (args.metrics or args.update_metrics):
        parser.error("--contribution-amount requires --metrics or --update-metrics")

    if args.no_export and not (args.metrics or args.update_metrics):
        parser.error("--no-export requires --metrics or --update-metrics")

    if args.holding_source != "transactions" and not (
        args.holdings or args.update_holdings or args.metrics or args.update_metrics
    ):
        parser.error("--holding-source requires holdings or metrics mode")

    if args.json and not (args.holdings or args.update_holdings):
        parser.error("--json requires --holdings or --update-holdings")

    if args.update_holdings or args.update_policy or args.update_metrics:
        run_data_pipeline(db_path=args.db_path)

    if args.holdings or args.update_holdings:
        try:
            holdings = get_current_holdings(
                db_path=args.db_path,
                source=args.holding_source,
            )
            if args.ticker:
                holdings = _filter_holdings_by_ticker(holdings, args.ticker)
                if not holdings:
                    print(
                        f"{normalize_ticker(args.ticker)} was not found in current holdings",
                        file=sys.stderr,
                    )
                    return 1
        except ValueError as exc:
            print(str(exc), file=sys.stderr)
            return 1
        except Exception as exc:
            print(f"Current holdings failed: {exc}", file=sys.stderr)
            return 1
        finally:
            close_connection()

        if args.json:
            print(json.dumps(holdings, indent=2, default=str))
        else:
            print(format_holdings_table(holdings))
        return 0

    if args.policy or args.update_policy:
        try:
            active_grouping = run_policy_grouping(
                ticker=args.ticker,
                db_path=args.db_path,
                export_path=args.export_path,
            )
        except ValueError as exc:
            print(exc)
            return 1
        except Exception as exc:
            print(f"Policy grouping failed: {exc}")
            return 1
        finally:
            close_connection()

        print(format_grouping_table(active_grouping))
        print(f"\nJSON export: {active_grouping['export_path']}")
        print("Database table updated: portfolio_grouping_active")
        return 0

    if args.metrics or args.update_metrics:
        try:
            metrics = run_portfolio_metrics(
                ticker=args.ticker,
                db_path=args.db_path,
                export_path=args.export_path,
                contribution_amount=args.contribution_amount,
                export=not args.no_export,
                holding_source=args.holding_source,
            )
        except ValueError as exc:
            print(exc)
            return 1
        except Exception as exc:
            print(f"Portfolio metrics failed: {exc}")
            return 1
        finally:
            close_connection()

        print(format_metrics_output(metrics))
        if not args.no_export:
            print(f"\nJSON export: {metrics['export_path']}")
        return 0

    run_data_pipeline(db_path=args.db_path)
    return 0
        
            
if __name__ == "__main__":
    raise SystemExit(main())
