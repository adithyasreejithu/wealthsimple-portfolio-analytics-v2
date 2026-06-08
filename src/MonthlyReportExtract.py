import io, re, os
from pathlib import Path
import fitz
import pytesseract
import pandas as pd
from PIL import Image
from dotenv import load_dotenv
from Config import *
from system_logger import get_logger
from concurrent.futures import ProcessPoolExecutor, as_completed

logger = get_logger(__name__)

load_dotenv()

# Completed
def check_data_files():
    """
    Retrieves wealthsimple files that exist with the folder

    Returns: 
        list: all pdf files
    """
    data_files = os.getenv("DATA_FILES")

    if not data_files:
        raise EnvironmentError("DATA_FILES environment variable is not set")
    df_path = Path(data_files)

    df_path = Path(data_files)
    read_df_path = df_path / "Read_Files"
    read_df_path.mkdir(parents=True, exist_ok=True)
    files = list(df_path.glob("*.pdf"))
    return files

def move_read_file(file: Path):
    """
    Function resposible for moving files that have been read
    """
    subfolder = file.parent / "Read_Files"
    subfolder.mkdir(exist_ok=True)

    new_path = subfolder / file.name

    if new_path.exists():
        raise FileExistsError(f"{new_path} already exists")

    logger.info(f"Moving {file.name} to {subfolder}")
    file.rename(new_path)


def ocr_extract(page_num, file_path):
    """
    OCR Extracts text from indiviudal page. 

    Returns: 
        int: page number 
        str: string of the page data
    """
    with fitz.open(file_path) as doc: 
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=DPI)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = pytesseract.image_to_string(img,config=PSM_MODE)
        return page_num, text
    
# def parse_transactions(text: str):
#     pattern = get_pattern()
#     matches = [m.groupdict() for m in pattern.finditer(text)]

#     if not matches:
#         # print(text)
#         return pd.DataFrame()

#     df = pd.DataFrame(matches)
  
#     return df

# def parse_transactions(text: str):
#     # collapse line breaks / weird OCR spacing into one line
#     row = " ".join(text.split())

#     date_match = re.search(r"\b(\d{4}-\d{2}-\d{2})\b", row)
#     type_match = re.search(r"\b(BUY|SELL|DIV|CONT|LOAN|RECALL|FPLINT|NRT)\b", row)
#     symbol_match = re.search(r"\b(?:BUY|SELL|DIV|CONT|LOAN|RECALL|FPLINT|NRT)\s+([A-Z0-9]+)\b", row)

#     quantity_match = re.search(r"\b(?:Bought|Sold)\s+(\d+(?:\.\d+)?)\s+shares\b", row)
#     exec_match = re.search(r"executed(?:\s*at)?[\s\S]*?(\d{4}-\d{2}-\d{2})", row)
#     money_match = re.search(r"\$?(\d+\.\d{2})\s+\$?(\d+\.\d{2})\s+\$?(\d+\.\d{2})", row)

#     name_match = re.search(
#         r"\b(?:BUY|SELL|DIV|CONT|LOAN|RECALL|FPLINT|NRT)\s+[A-Z0-9]+\s*-\s*(.*?)(?=\$?\d+\.\d{2}\s+\$?\d+\.\d{2}\s+\$?\d+\.\d{2}|Bought|Sold|Cash dividend|Loan of|Shares on loan|$)",
#         row
#     )

#     data = {
#         "Date": [date_match.group(1) if date_match else None],
#         "Type": [type_match.group(1) if type_match else None],
#         "Symbol": [symbol_match.group(1) if symbol_match else None],
#         "Name": [name_match.group(1).strip() if name_match else None],
#         "Quantity": [quantity_match.group(1) if quantity_match else None],
#         "ExecDate": [exec_match.group(1) if exec_match else None],
#         "Debit": [money_match.group(1) if money_match else None],
#         "Credit": [money_match.group(2) if money_match else None],
#         "Balance": [money_match.group(3) if money_match else None],
#         "FXRate": [None],
#     }

#     # if we basically found nothing useful, return empty df
#     if not any(data[col][0] is not None for col in ["Date", "Type", "Symbol", "Debit", "Credit", "Balance"]):
#         return pd.DataFrame()

#     return pd.DataFrame(data)

def parse_transactions(text: str):
    print(text)



# In progress
def extraction_pipline(file):
    logger.info("started extraction process")
    logger.info(f"Reading {file.name}")
    
    # opening files
    with fitz.open(file) as pdf: 
        page_count = pdf.page_count
    
    # results dictionary to store OCR output values 
    results = {}

    # runnning parallel process to extract data
    with ProcessPoolExecutor(max_workers= CPU_WORKERS) as executor:
        # returns ocr extract for page i and file
        futures = [executor.submit(ocr_extract,i,file) for i in range(page_count)]

        for future in as_completed(futures):
            try:
                page_num, text = future.result()
                results[page_num] = text # storing page number as key and text as value in dict
            except Exception as e:
                logger.error("OCR failed on a page of %s: %s", file.name, e, exc_info=True)

    page_contents = "".join(results[i] for i in sorted(results.keys()))

    # Data Parsing and Cleaning
    mydict = {} 
    pattern = r"({})".format("|".join(map(re.escape, PDF_HEADINGS)))
    matches = list(re.finditer(pattern,page_contents))

    # Grouping content
    for i, match in enumerate(matches):
        current = match.group()
        key = f"{current}_{i+1}"
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(page_contents)
        content = page_contents[start:end]
        mydict[key] = content

    # Merging duplicate sections across different pages
    combined_activity = "\n".join(
        mydict[k] for k in mydict if k.startswith("Activity - Current period")
    )

    # Data Transformation
    all_dfs = []

    lines = combined_activity.splitlines()

    for i, line in enumerate(lines):
        full_line = line
        # print(line)

        match line:

            case _ if  any(word in line for word in ["BUY","DIV" ]):
                if i + 1 < len(lines):  # avoid out-of-range error
                    next_line = lines[i + 1].strip()
                    # print("!!!!!!" + next_line)
                    if not any(word in next_line for word in TRANSACTION_TYPES):
                        full_line += " " + next_line
                
                df = parse_transactions(full_line)

                # if not df.empty:
                #     all_dfs.append(df)         
            
            case _ if "CONT" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)

            case _ if "SELL" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)
            
            case _ if "LOAN" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)

            case _ if "NRT" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)
            
            case _ if "RECALL" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)

            case _ if "FPLINT" in line: 
                df = parse_transactions(full_line)
                all_dfs.append(df)

    if not all_dfs:
        logger.warning("No transactions parsed from %s", file.name)
        return pd.DataFrame()

    return pd.concat(all_dfs, ignore_index=True)

from multiprocessing import freeze_support

if __name__ == "__main__":
    # freeze_support()  

    # file = Path("C:\\Projects\\Python\\wealthsimple-portfolio-analytics-v2\\Data\\Data_Files\\2026-01.pdf")
    
    # df = extraction_pipline(file)

    # print(df.head(50))
    files = check_data_files()
    # print(type(file) for file in files)


