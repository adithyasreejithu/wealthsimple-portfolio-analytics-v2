from Database_Schema import initialize_database, get_connection, close_connection
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline, move_read_file
from yfinance_gather_security_info import get_security_history
from system_logger import get_logger

logger = get_logger(__name__)

def ocr_method(con) -> None:
    """
    Runs the OCR extraction pipeline for any new monthly report files.
    
    """
    # File check 
    file_list = check_data_files()

    if not file_list:
        logger.info("No new files found")
        return

    logger.info("New file(s) found: %s", len(file_list))

    # Pipeline for every new file added
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


def email_method(con) -> None:
    """Placeholder for email-based transaction ingestion."""
    pass


def main() -> None:
    logger.info("Process started")

    initialize_database()

    try:
        with get_connection() as con:
            ocr_method(con)

            hist_data = get_security_history(con)
            if hist_data is not None: 
                (hist_data, con)
                
            email_method(con)

    finally:
        close_connection()
        logger.info("Process completed")


if __name__ == "__main__":
    main()