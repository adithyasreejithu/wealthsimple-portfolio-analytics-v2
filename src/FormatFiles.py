import sys 
import os 
import re
from pathlib import Path
from dotenv import load_dotenv
from datetime import datetime
from system_logger import get_logger
from MonthlyReportExtract import check_data_files

logger = get_logger(__name__)
# DATA_FILES = os.getenv("DATA_FILES")

def rename_file(): 
    """
    Renames WS pdf files that exist within the data folder and renames it to only have order 
    """
    files = check_data_files()
    logger.info("Checking %d data file(s) for Wealthsimple naming cleanup", len(files))
    
    for file in files:
        # looking for year month in file name
        match = re.search(r"\d{4}-\d{2}",file.name)

        if not match:
            logger.warning(f"No YYYY-MM found in {file.name}")
            continue

        year_month = match.group()
        logger.info("Found new file with WS naming convention %s", year_month)

        # creates new file name adding extracted file name with file suffix(pdf)
        new_name = f"{year_month}{file.suffix}"
        new_path = file.with_name(new_name) # new file path with same dir and new name

        if new_path.exists():
            logger.info("File already has target name or target exists: %s", new_name)
            continue

        logger.info("Renaming %s to %s", file.name, new_name)
        file.rename(new_path)

# rename_file()

