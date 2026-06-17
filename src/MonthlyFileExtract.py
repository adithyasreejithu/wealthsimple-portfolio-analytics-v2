import re 
import os 
import logging
import camelot
import pdfplumber 
import pandas as pd
from pathlib import Path 
from Config import get_pattern_two, get_B_types, get_settlement_keywords, get_trans_req
from system_logger import get_logger

logger = get_logger(__name__)


MONEY_COLUMNS = ["debit", "credit", "balance"]


def _money_series(data: pd.DataFrame, column: str) -> pd.Series:
    if column not in data.columns:
        return pd.Series(dtype="float64")
    cleaned = (
        data[column]
        .astype("string")
        .str.replace(",", "", regex=False)
        .str.replace("$", "", regex=False)
        .str.strip()
    )
    return pd.to_numeric(cleaned, errors="coerce")


def _has_text_value(data: pd.DataFrame, column: str) -> pd.Series:
    if column not in data.columns:
        return pd.Series(False, index=data.index)
    values = data[column].astype("string").str.strip()
    return values.notna() & values.ne("")


def _money_summary(data: pd.DataFrame) -> dict:
    summary = {"rows": len(data)}
    for column in MONEY_COLUMNS:
        values = _money_series(data, column)
        present = _has_text_value(data, column)
        summary[f"{column}_present"] = int(present.sum())
        summary[f"{column}_missing"] = int((~present).sum())
        summary[f"{column}_total"] = float(values.fillna(0).sum())
    return summary


def _log_money_summary(stage: str, data: pd.DataFrame, level: int = None) -> None:
    log_level = level if level is not None else logger.level
    logger.log(log_level, "%s money summary: %s", stage, _money_summary(data))


def _missing_value_mask(values: pd.Series) -> pd.Series:
    text_values = values.astype("string").str.strip()
    return values.isna() | text_values.isna() | text_values.eq("")


def _fill_missing_dividend_exec_dates(data: pd.DataFrame) -> pd.DataFrame:
    if not {"transaction", "execDate", "date"}.issubset(data.columns):
        return data

    data = data.copy()
    transaction_type = data["transaction"].astype("string").str.strip().str.upper()
    missing_exec_date = _missing_value_mask(data["execDate"])
    fill_mask = transaction_type.eq("DIV") & missing_exec_date & ~_missing_value_mask(data["date"])

    if fill_mask.any():
        data.loc[fill_mask, "execDate"] = data.loc[fill_mask, "date"]
        logger.info(
            "Filled missing dividend execDate values from date | rows=%d",
            int(fill_mask.sum()),
        )

    return data


def clean_transactions(data: pd.DataFrame):
    """
    1. Remove Transactions for Future Settlement
    2. Check what rows have missing data
    """
    logger.info("Starting transaction cleaning | rows=%d", len(data))
    _log_money_summary("Before cleaning", data, logging.INFO)

    def confirm_data(data):
        invalid_indexes = []
        required_cols = get_trans_req()

        for trans, cols in required_cols.items():
            subset = data.loc[data['transaction'].eq(trans)]
            missing_required = pd.Series(False, index=subset.index)
            for col in cols:
                if col not in subset.columns:
                    missing_required = pd.Series(True, index=subset.index)
                    break
                missing_required = missing_required | _missing_value_mask(subset[col])
            invalid_indexes.extend(subset.index[missing_required])

        invalid_rows = data.loc[invalid_indexes]
        if not invalid_rows.empty:
            logger.warning(
                "Dropped %d rows missing required fields | sample=%s",
                len(invalid_rows),
                invalid_rows.head(5).to_dict(orient="records"),
            )
        cleaned = data.drop(index=invalid_indexes).reset_index(drop=True)
        return cleaned  # FIX 1: was missing return

    def remove_settlements(data):
        keywords = get_settlement_keywords()
        pattern = "|".join(re.escape(k) for k in keywords)
        filt = data.astype(str).apply(lambda col: col.str.contains(pattern))
        result = data[filt.any(axis=1)]
        data = data[~filt.any(axis=1)]

        if not result.empty:
            logger.warning(
                "Dropped %d settlement/header rows due to keyword match | sample=%s",
                len(result),
                result.head(5).to_dict(orient="records"),
            )

        has_money_value = pd.Series(False, index=data.index)
        for column in MONEY_COLUMNS:
            has_money_value = has_money_value | _has_text_value(data, column)

        kept = data[has_money_value]
        dropped = data[~has_money_value]
        if not dropped.empty:
            logger.warning(
                "Dropped %d rows with no debit, credit, or balance values | sample=%s",
                len(dropped),
                dropped.head(5).to_dict(orient="records"),
            )

        return kept

    data = _fill_missing_dividend_exec_dates(data)
    data = remove_settlements(data)
    cleaned = confirm_data(data)
    logger.info("Transaction cleaning complete | rows=%d", len(cleaned))
    _log_money_summary("After cleaning", cleaned, logging.INFO)
    return cleaned


def merge_text(grp):
    return " ".join(grp.dropna().astype(str).str.strip())


def transformations(df: pd.DataFrame):
    '''
    Handles all transformations related to the extracted wealthsimple PDF extraction

    returns  
        extracted: pd.Dataframe - for the specific file 
    '''

    # FIX 2: normalize columns to integer RangeIndex before anything else,
    # because pd.concat across multiple tables can scramble the column index
    df.columns = range(len(df.columns))
    df = df.reset_index(drop=True)
    logger.debug("transformations() received df shape=%s columns=%s", df.shape, df.columns.tolist())

    def formatting(df: pd.DataFrame):
        '''
        Handles basic formatting of the dataframe. Renames last 3 cols and drops header row.
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

        df["cont"] = df[0].notna().cumsum()  # safe now — column 0 guaranteed by FIX 2

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

        logger.info(
            "Extracted transaction fields | rows=%d | transactions=%s",
            len(df),
            df["transaction"].dropna().value_counts().to_dict(),
        )
        _log_money_summary("After extraction", df, logging.DEBUG)

        return df

    df = merge_rows(df)
    cleaned = formatting(df)
    extracted = extraction(cleaned)

    return extracted


def read_table(table):
    logger.info("Parsing Report %s", table.parsing_report)
    data = table.df

    # Drop the camelot header rows
    data = data.drop([0, 1, 2])

    # FIX 3: reset both column names and row index after drop
    data.columns = range(len(data.columns))
    data = data.reset_index(drop=True)

    return data


def find_activity_pages(file: Path, search="Activity - Current period"):
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
        return transactions  # FIX 4: return empty DataFrame, not empty list

    all_tables = []

    for page in pages_list:
        logger.info("Reading page %s from %s", page, file.name)

        # First pass: find how many tables are on this page
        tables = camelot.read_pdf(
            str(file),
            pages=str(page),
            flavor="stream",
        )
        n_tables = len(tables)

        # Second pass: supply one column spec per table found
        tables = camelot.read_pdf(
            str(file),
            pages=str(page),
            flavor="stream",
            columns=["78,126,444,478.5,517"] * n_tables  # repeat for each table
        )

        all_tables.extend(tables)
        logger.info("Found %s total table(s)", len(all_tables))

    for i, table in enumerate(all_tables):
        df = table.df
        if df.astype(str).stack().str.contains("Activity - Current period", na=False).any():
            matched_table.append((i, table))

    logger.info("Kept %s table(s)", len(matched_table))

    # FIX 6: guard against tables with unexpected column counts before concat
    EXPECTED_COLS = 6  # camelot stream with 5 column splits produces 6 columns
    for id, table in matched_table:
        x = read_table(table)
        if len(x.columns) != EXPECTED_COLS:
            logger.warning(
                "Skipping table %s — unexpected column count: %s (expected %s)",
                id, len(x.columns), EXPECTED_COLS
            )
            continue
        logger.debug("Table %s shape: %s columns: %s", id, x.shape, x.columns.tolist())
        transactions = pd.concat([transactions, x], ignore_index=True)

    if transactions.empty:
        logger.warning("No valid tables extracted from %s", file.name)
        return transactions

    transactions = transformations(transactions)
    transactions = transactions[["date", "transaction", "ticker_id", "quantity", "execDate", "fx_rate", "debit", "credit", "balance"]]

    final_df = clean_transactions(transactions)
    logger.info(
        "Camelot extraction complete | file=%s | rows=%d",
        file.name,
        len(final_df),
    )
    return final_df

# if __name__ == "__main__":
#     file = Path("C:\\Projects\\Python\\wealthsimple-portfolio-analytics-v2\\Data\\Data_Files\\2025-06.pdf")
#     camelot_extraction_pipeline(file)
