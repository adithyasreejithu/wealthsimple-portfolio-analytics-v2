from Database_Schema import initialize_database, get_connection, close_connection
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline, move_read_file
from MonthlyFileExtract import camelot_extraction_pipeline
from concurrent.futures import ProcessPoolExecutor
from yfinance_gather_security_info import get_security_history
from system_logger import get_logger
from Purchase_Validation import email_handler
from FormatFiles import rename_file
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


def main() -> None:
    logger.info("Data pipeline initiated")

    initialize_database()

    try:
        with get_connection() as con:
            data = camelot_method()
            upload_transactions(data, con)
    except Exception:
        logger.exception("Transaction upload failed")
    finally:
        close_connection()
        logger.info("Transaction process completed")

    try:
        with get_connection() as con:
            hist_data = get_security_history(con)
            if hist_data is not None:
                upload_history(hist_data, con)
            email_method(con)
    except Exception:
        logger.exception("Transaction upload failed")
    finally:
        close_connection()
        logger.info("Process completed")
        
            
if __name__ == "__main__":
    main()

# notes for fixes need to have a fix for when everuthing is empty
# need to fix and catch stock history's that are failing 