import argparse
from Database_Schema import initialize_database, get_connection, close_connection
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline, move_read_file
from MonthlyFileExtract import camelot_extraction_pipeline
from concurrent.futures import ProcessPoolExecutor
from yfinance_gather_security_info import get_security_history
from system_logger import get_logger
from Purchase_Validation import email_handler
from FormatFiles import rename_file
from run_portfolio_policy import format_grouping_table, run_policy_grouping
import pandas as pd

logger = get_logger(__name__)


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
    data = camelot_extraction_pipeline(file)
    move_read_file(file)
    print(data)
    return data


def camelot_method() -> pd.DataFrame:

    rename_file()

    file_list = check_data_files()

    if not file_list:                          # FIX 1: was returning on files found, not on empty
        logger.info("No new files found")
        return

    logger.info("New file(s) found: %s", len(file_list))

    with ProcessPoolExecutor() as executor:
        results = list(executor.map(process_pdf, file_list))   # FIX 2: removed duplicate map call

    print(results)

    valid = [r for r in results if r is not None and not r.empty]

    if not valid:
        logger.warning("No valid results to upload")
        return

    combined = pd.concat(valid, ignore_index=True)
    combined.to_csv("Upload.csv", index=False)
    logger.info("Saved %s rows to Upload.csv", len(combined))

    return combined


def email_method(con) -> None:
    """Placeholder for email-based transaction ingestion."""
    email_handler(con)


def _connection_for_path(db_path: str | None = None):
    if db_path is None:
        return get_connection()
    return get_connection(db_path)


def run_data_pipeline(db_path: str | None = None) -> None:
    logger.info("Data pipeline initiated")

    if db_path is None:
        initialize_database()
    else:
        initialize_database(db_path)

    try:
        with _connection_for_path(db_path) as con:
            data = camelot_method()
            upload_transactions(data, con)
    except Exception:
        logger.exception("Transaction upload failed")
    finally:
        close_connection()
        logger.info("Transaction process completed")

    try:
        with _connection_for_path(db_path) as con:
            hist_data = get_security_history(con)
            if hist_data is not None:
                upload_history(hist_data, con)
            email_method(con)
    except Exception:
        logger.exception("Transaction upload failed")
    finally:
        close_connection()
        logger.info("Process completed")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run the Wealthsimple portfolio data pipeline.",
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
        "--ticker",
        help="Only run policy grouping for one ticker. Requires --policy or --update-policy.",
    )
    parser.add_argument(
        "--db-path",
        help="DuckDB path to use for the pipeline and policy grouping.",
    )
    parser.add_argument(
        "--export-path",
        help="JSON export path to use for policy grouping.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    if args.policy and args.update_policy:
        parser.error("Use either --policy or --update-policy, not both")

    if args.ticker and not (args.policy or args.update_policy):
        parser.error("--ticker requires --policy or --update-policy")

    if args.export_path and not (args.policy or args.update_policy):
        parser.error("--export-path requires --policy or --update-policy")

    if args.update_policy:
        run_data_pipeline(db_path=args.db_path)

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

    run_data_pipeline(db_path=args.db_path)
    return 0
        
            
if __name__ == "__main__":
    raise SystemExit(main())

# notes for fixes need to have a fix for when everuthing is empty
# need to fix and catch stock history's that are failing 
