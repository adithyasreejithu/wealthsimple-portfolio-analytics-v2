import os
import re
from datetime import date, datetime

import pandas as pd
from dotenv import load_dotenv
from imap_tools import AND, MailBox

from system_logger import get_logger


load_dotenv()

GMAIL = os.getenv("GMAIL_USER")
PASS = os.getenv("GMAIL_PASS")
INTERAC_SENDER = "catch@payments.interac.ca"
INTERAC_SUBJECT = "Interac e-Transfer"
INTERAC_SUBJECT_PATTERN = re.compile(
    r"^Interac e-Transfer:\s*ADITHYA SREEJITHU PANICKER\b",
    flags=re.IGNORECASE,
)
EMAIL_TICKER_ID = 0
EMAIL_TICKER = "EMAIL"
EMAIL_ACCOUNT = "TFSA"
EMAIL_TRANSACTION = "Deposit"

logger = get_logger(__name__)


def _normalize_email_text(email: str) -> str:
    return "\n".join(line.strip() for line in email.splitlines() if line.strip())


def _extract_money(email: str):
    text = _normalize_email_text(email)
    patterns = [
        r"(?:Amount|Deposit|Deposited|Sent|Received|Transfer)\D{0,40}(?:CA\$|CAD|\$)?\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
        r"(?:CA\$|CAD|\$)\s*([0-9][0-9,]*(?:\.[0-9]{1,2})?)",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if match:
            return match.group(1).replace(",", "")

    return None


def _extract_date(email: str, fallback_date=None):
    text = _normalize_email_text(email)
    patterns = [
        r"(?:Date|Deposited on|Received on|Sent on|Transfer date)\D{0,40}([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"(?:Date|Deposited on|Received on|Sent on|Transfer date)\D{0,40}(\d{4}-\d{1,2}-\d{1,2})",
        r"(?:Date|Deposited on|Received on|Sent on|Transfer date)\D{0,40}(\d{1,2}/\d{1,2}/\d{4})",
        r"([A-Z][a-z]+ \d{1,2}, \d{4})",
        r"(\d{4}-\d{1,2}-\d{1,2})",
        r"(\d{1,2}/\d{1,2}/\d{4})",
    ]

    for pattern in patterns:
        match = re.search(pattern, text, flags=re.IGNORECASE)
        if not match:
            continue

        parsed = pd.to_datetime(match.group(1), errors="coerce")
        if not pd.isna(parsed):
            return parsed.date()

    return fallback_date


def parse_email_content(email: str, received_date=None, subject: str = "") -> pd.DataFrame | None:
    try:
        searchable_text = "\n".join(part for part in (subject, email) if part)
        debit = _extract_money(searchable_text)
        parsed_date = _extract_date(searchable_text, received_date)

        row = {
            "account": EMAIL_ACCOUNT,
            "transaction": EMAIL_TRANSACTION,
            "ticker_id": EMAIL_TICKER_ID,
            "ticker": EMAIL_TICKER,
            "quantity": 0,
            "avg_price": 0,
            "total_cost": 0,
            "debit": debit,
            "date": parsed_date,
        }

        logger.debug(
            "Parsed Interac email deposit | debit=%s | date=%s",
            row["debit"],
            row["date"],
        )
        return pd.DataFrame([row])

    except Exception as exc:
        logger.error("Interac email parse error: %s", exc)
        return None


def _message_summary(msg, parsed_df: pd.DataFrame) -> dict:
    row = parsed_df.iloc[0].to_dict()
    return {
        "from": getattr(msg, "from_", INTERAC_SENDER),
        "subject": getattr(msg, "subject", None),
        "received_date": msg.date.date() if getattr(msg, "date", None) else None,
        "parsed_date": row.get("date"),
        "debit": row.get("debit"),
    }


def _message_from_matches(msg) -> bool:
    sender = str(getattr(msg, "from_", "") or "").lower()
    return INTERAC_SENDER.lower() in sender


def _message_subject_matches(msg) -> bool:
    subject = str(getattr(msg, "subject", "") or "").strip()
    return bool(INTERAC_SUBJECT_PATTERN.search(subject))


def find_interac_emails() -> pd.DataFrame:
    ensemble = []
    summaries = []

    logger.info("Starting read-only Interac email scan")

    with MailBox("imap.gmail.com").login(GMAIL, PASS, "Inbox") as mb:
        for msg in mb.fetch(
            AND(
                from_=INTERAC_SENDER,
                subject=INTERAC_SUBJECT,
            )
        ):
            if not _message_from_matches(msg):
                continue

            if not _message_subject_matches(msg):
                continue

            received_date = msg.date.date() if msg.date else None
            parsed = parse_email_content(
                msg.text,
                received_date,
                getattr(msg, "subject", ""),
            )
            if parsed is not None:
                ensemble.append(parsed)
                summaries.append(_message_summary(msg, parsed))

    if not ensemble:
        logger.info("No Interac emails found")
        return pd.DataFrame()

    df = pd.concat(ensemble, ignore_index=True)
    logger.info("Interac email scan complete | found=%d", len(df))

    for summary in summaries:
        print(summary)

    return df


if __name__ == "__main__":
    found = find_interac_emails()

    if found.empty:
        print(f"No emails found from {INTERAC_SENDER}")
    else:
        print(found.to_string(index=False))
