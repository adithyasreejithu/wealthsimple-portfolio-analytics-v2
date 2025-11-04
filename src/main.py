from Database_Schema import initialize_database
from Database_Upload import upload_transactions
from MonthlyReportExtract import check_data_files, extraction_pipline

def main():
    # Connecting to databases 
    initialize_database    

    # Run extraction Pipeline 
    file_list =  check_data_files()
    for file in file_list:
        trans_df = extraction_pipline(file)
        upload_transactions(trans_df)
        

        

if __name__ == "__main__":
    main()