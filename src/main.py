from Database_Schema import*
from Database_Upload import upload_transactions
from MonthlyReportExtract import check_data_files, extraction_pipline

def main():   
    # reset_database()

    # Connecting to databases 
    initialize_database() 
    # Run extraction Pipeline 
    file_list =  check_data_files()
    for file in file_list:
        print(file)
        trans_df = extraction_pipline(file)
        upload_transactions(trans_df)
                

if __name__ == "__main__":
    main()