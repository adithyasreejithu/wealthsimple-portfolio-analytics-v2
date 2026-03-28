from Database_Schema import initialize_database
from system_logger import get_logger
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline, move_read_file
from yfinance_gather_security_info import get_security_history

logger = get_logger(__name__)

def main(): 
    logger.info("Started Process")
    # reset_database()

    # Connecting to databases 
    initialize_database() 
    
    # add file renaming function

    # Run extraction Pipeline 
    file_list =  check_data_files()

    if file_list: 
        logger.info("New file(s) found %s", len(file_list))
        
        for file in file_list: 
            try: 
                trans_df = extraction_pipline(file)

                if trans_df is None or trans_df.empty:
                    logger.warning("Empty DF file skipped: {file} ")
                    continue

                print(trans_df)

                upload_transactions(trans_df)
                move_read_file(file)
            
            except Exception as e: 
                logger.error(f"Failed to process file {file} {e}" , exc_info=True)
            
    else: 
        logger.info("No new files found")

    hist_data = get_security_history()
    upload_history(hist_data)

    logger.info("Process completed")
    
if __name__ == "__main__":
    main()