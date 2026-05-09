\
import re 
import os 
import camelot
import pdfplumber 
import pandas as pd
from pathlib import Path 
from Config import get_pattern_two, get_B_types, get_settlement_keywords, get_trans_req
from system_logger import get_logger

logger = get_logger(__name__)

def clean_transactions(data: pd.DataFrame):
    """
    1. Remove Transactions for Future Settlement
    2. Check what rows have missing data
    """

    def confirm_data(data): 
        invalid_indexes = []

        required_cols = get_trans_req()

        for trans, cols in required_cols.items():
            subset = data.loc[data['transaction'].eq(trans)]

            missing_required = subset[cols].isna().any(axis=1)

            invalid_indexes.extend(subset.index[missing_required])

        invalid_rows = data.loc[invalid_indexes]
        logger.warning("Dropped rows containing invalid rows \n %s", invalid_rows)
        cleaned = data.drop(index=invalid_indexes).reset_index(drop=True)

    def remove_settlements(data):
        keywords = get_settlement_keywords()
        pattern = "|".join(re.escape(k) for k in keywords)
        filt = data.astype(str).apply(lambda col: col.str.contains(pattern))
        data = data[~filt.any(axis=1)]
        result = data[filt.any(axis=1)]

        logger.warning("Dropped rows due to keywords caught \n %s", result)

        # Future settlements do not have a debit column 
        filt2 = data["debit"] != ""
        kept = data[filt2]
        dropped = data[~filt2]

        if not dropped.empty:
            logger.warning("Dropped rows contain future settlement criteria \n %s", dropped)

        return kept

    data = remove_settlements(data)
    cleaned = confirm_data(data)

    return data



def merge_text(grp):
    return " ".join(grp.dropna().astype(str).str.strip())

def transformations(df: pd.DataFrame):
    '''
    Handles all transformations related to the extracted wealthsimple PDF extraction

    returns  
        extracted: pd.Dataframe - for the specfic file 
    '''

    def formatting(df: pd.DataFrame):
        '''
        Handles basic formatting of the dataframe. Numbers sliced data rows to put together
        '''
        cols = list(df.columns)
        non_crit_cols = [c for c in cols if c != "cont"]

        last3 = non_crit_cols[-3:]
        rename_map = dict(zip(last3, ["debit", "credit", "balance"]))
        df = df.rename(columns=rename_map)

        df = df.drop(0).reset_index(drop=True)

        return df

    def merge_rows(df: pd.DataFrame):
        """
        Groups the sliced rows together 
        """
        df = df.replace(r'^\s*$', pd.NA, regex=True)

        df["cont"] = df[0].notna().cumsum()

        agg_dict = {
            col: merge_text
            for col in df.columns
            if col != "cont"
        }

        df = (
            df.groupby("cont", as_index=False)
            .agg(agg_dict)
            .drop(columns="cont")
            .reset_index(drop=True)
        )

        return df

    def extraction(df: pd.DataFrame):
        text_cols = [c for c in df.columns if c not in ["debit", "credit", "balance"]]
        df["merged"] = df[text_cols].astype(str).agg(" ".join, axis=1)

        extracted = df["merged"].str.extract(
            get_pattern_two(),
            flags=re.VERBOSE
        )

        df["quantity"] = (
            extracted["quantity_buy"]
            .fillna(extracted["quantity_loan"])
            .fillna(extracted["quantity_recall"])
        )

        df["execDate"] = (
            extracted['execDate']
            .fillna(extracted['record_date'])
        )

        df["date"] = extracted["date"]
        df["transaction"] = extracted["transaction"]
        df["ticker_id"] = extracted["ticker_id"]
        df["fx_rate"] = extracted["fx_rate"]
        df["record_date"] = extracted["record_date"] 

        return df

    df = merge_rows(df)
    cleaned = formatting(df)
    extracted = extraction(cleaned)

    return extracted

def read_table(table):
    logger.info("Parsing Report %s", table.parsing_report)
    data = table.df

    # Starting Data cleasing 
    data = data.drop([0,1,2])

    return data
    

def find_activity_pages(file: Path,  search= "Activity - Current period"):
    """
    Uses PDFPlumber to find all pages that contain search word. 
    
    Return: 
    matched_text: list[str]
    """

    matched_text = [] 

    with pdfplumber.open(file) as pdf:

        for i, page in enumerate(pdf.pages, start=1):
            text = page.extract_text() or ""

            if search in text: 
                matched_text.append(str(i))

    return matched_text

def camelot_extraction_pipeline(file: Path):

    # variables 
    matched_table = []
    transactions = pd.DataFrame()
    pages_list = find_activity_pages(file)

    # Checking if pages do not exist
    if not pages_list: 
        logger.info("No pages found with Keyword on page(s) %s", file.name)
        return matched_table
    
    # joining all pages together 
    pages_concat = ",".join(pages_list)    
    logger.info("Found %s pages that contain keyword on page(s) %s", len(pages_list), pages_concat)

    # Find tables on all pages 
    tables = camelot.read_pdf(file, pages=pages_concat, flavor='stream', columns=["78,126,444,478.5,517"]* len(pages_list))
    logger.info("Reading WS File %s", file.name)

    for i, table in enumerate(tables):
        df = table.df
        # Find tables that contain Activity heading
        if df.astype(str).stack().str.contains("Activity - Current period", na=False).any():
            matched_table.append((i, table))

    logger.info("Kept %s table(s)", len(matched_table))

    for id, table in matched_table: 
        x = read_table(table)
        transactions = pd.concat([transactions,x], ignore_index=True)
    
    transactions = transformations(transactions)
    transactions = transactions[["date", "transaction", "ticker_id", "quantity", "execDate", "fx_rate", "debit","credit","balance"]]
    
    final_df = clean_transactions(transactions)
    return final_df

# if __name__ == "__main__":
#     file = Path("C:\\Projects\\Python\\wealthsimple-portfolio-analytics-v2\\Data\\Data_Files\\2025-06.pdf")
#     camelot_extraction_pipeline(file)