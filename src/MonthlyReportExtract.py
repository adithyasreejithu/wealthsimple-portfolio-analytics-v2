import io, re, os
from pathlib import Path

import fitz
import pytesseract
from PIL import Image

from dotenv import load_dotenv
from collections import defaultdict

from concurrent.futures import ProcessPoolExecutor, as_completed

load_dotenv()
DATA_FILES = os.getenv("DATA_FILES")
DPI = 300
PSM_MODE = "--psm 6"
CPU_WORKERS = os.cpu_count() -1
PDF_HEADINGS = ["Portfolio Cash", "Portfolio Equities", "Activity - Current period","Transactions for Future Settlement"]

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

    for i, match in enumerate(matches):
        current = match.group()
        key = f"{current}_{i+1}"
        start = match.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(page_contents)
        content = page_contents[start:end]
        mydict[key] = content

    activity_section = [
        key for key in mydict.keys()
        if key.startswith("Activity - Current period")
    ]

    for item in activity_section: 
        value = mydict[item]
        lines = value.splitlines()
        lines = lines[2:-4]

        for line in lines:
            print(line)
        print("-------------------------------------------\n")





    # combined_activity = "\n".join(
    #     mydict[k] for k in mydict if k.startswith("Activity - Current period")
    # )

    # print(combined_activity)

if __name__ == "__main__":
    # file_list = check_data_files() # Default case
    test_file = testingcase() # Testing Case

    # for file in file_list:
    #     print(file)

    extraction_pipline(test_file)
