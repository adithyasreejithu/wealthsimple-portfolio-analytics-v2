import io, re, os
from pathlib import Path

import fitz
import pytesseract
import pandas as pd
from PIL import Image

from dotenv import load_dotenv
from collections import defaultdict
from Config import *
from concurrent.futures import ProcessPoolExecutor, as_completed

  
load_dotenv()
DATA_FILES = os.getenv("DATA_FILES")


# Completed
def testingcase():
    ws = os.environ.get("DATA_FILES")
    testing_file = Path(ws) / "March_2025.pdf"
    return testing_file

# Completed
def check_data_files():
    df_path = Path(DATA_FILES)
    read_df_path = df_path / "Read_Files"
    read_df_path.mkdir(parents=True, exist_ok=True)
    files = list(df_path.glob("*.pdf"))
    return files

def ocr_extract(page_num, file_path):
    with fitz.open(file_path) as doc: 
        page = doc.load_page(page_num)
        pix = page.get_pixmap(dpi=DPI)
        img = Image.open(io.BytesIO(pix.tobytes("png")))
        text = pytesseract.image_to_string(img,config=PSM_MODE)
        return page_num, text
    
def parse_transactions(text: str):
    pattern = get_pattern()
    matches = [m.groupdict() for m in pattern.finditer(text)]

    if not matches:
        print(text)
        return pd.DataFrame()

    df = pd.DataFrame(matches)
  
    return df

def merge_df_lines(text: str) -> str : 
    merged_lines = []

    for line in text.splitlines():
        line = line.strip()
        if not line: 
            continue
        if line not in TRANSACTION_TYPES:
            merged_lines.append(line)
        else:
            if merged_lines:
                merged_lines.append(line)



# In progress
def extraction_pipline(file):
    # CPU Setup 
    with fitz.open(file) as pdf: 
        page_count = pdf.page_count
    
    results = {}
    with ProcessPoolExecutor(max_workers= CPU_WORKERS) as executor:
        futures = [executor.submit(ocr_extract,i,file) for i in range(page_count)]

        for future in as_completed(futures):
            page_num, text = future.result()
            results[page_num] = text

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

    # print(combined_activity)

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
                if not df.empty:
                    all_dfs.append(df)             
            
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


    if all_dfs:
        transaction_df = pd.concat(all_dfs, ignore_index=True)

    return transaction_df
    


# testing case
if __name__ == "__main__":
    test_file = testingcase() # Testing Case
    extraction_pipline(test_file)
