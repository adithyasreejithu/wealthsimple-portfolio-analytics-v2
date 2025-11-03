from MonthlyReportExtract import check_data_files, extraction_pipline

def main():
    # Connecting to databases 

    # Run extraction Pipeline 
    file_list =  check_data_files()
    for file in file_list:
        extraction_pipline(file)

if __name__ == "__main__":
    main()