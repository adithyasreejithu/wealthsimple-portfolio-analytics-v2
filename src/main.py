from Database_Schema import*
from system_logger import get_logger
from Database_Upload import upload_transactions, upload_history
from MonthlyReportExtract import check_data_files, extraction_pipline
from yfinance_gather_security_info import get_security_history

logger = get_logger(__name__)

def main():   
    logger.info("Started Process")
    # reset_database()

    # Connecting to databases 
    initialize_database() 
    
    # Run extraction Pipeline 
    file_list =  check_data_files()
    for file in file_list:
        
        trans_df = extraction_pipline(file)
        upload_transactions(trans_df)
        
    hist_data = get_security_history()
    upload_history(hist_data)

    logger.info("Process completed")
    

if __name__ == "__main__":
    main()