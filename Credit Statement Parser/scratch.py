import re
import sys
import os
import argparse
from glob import glob
from pathlib import Path
from datetime import datetime
import pdfplumber
import pandas as pd
# Add parent directory to sys.path
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
import db



# ─────────────────────────────────────────────────────────────────────────────
# CONFIGURATION – tweak these patterns to match your bank's format
# ─────────────────────────────────────────────────────────────────────────────

# Matches dates like: 01/23/2024  2024-01-23  Jan 23, 2024  23 Jan 2024
DATE_PATTERN = re.compile(
    r"""
    (?:
        \b(\d{1,2})\-(\d{1,2})\b                        # MM-DD (no year)
        |
        \b(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{2,4})\b   # MM/DD/YYYY or DD/MM/YYYY
        |
        \b(\d{4})[\/\-](\d{1,2})[\/\-](\d{1,2})\b      # YYYY-MM-DD
        |
        \b([A-Za-z]{3,9})\s+(\d{1,2}),?\s+(\d{4})\b     # Jan 23, 2024
        |
        \b(\d{1,2})\s+([A-Za-z]{3,9})\s+(\d{4})\b       # 23 Jan 2024
        |
        \b([A-Za-z]{3,9})\s+(\d{1,2})                  #Jan 01
    )
    """,
    re.VERBOSE,
)

# Matches monetary amounts like: 1,234.56  -50.00  ($200.00)  $1,000
AMOUNT_PATTERN = re.compile(
    r"[\(\-]?\$?[\d,]+\.\d{2}\)?"
)

# Column name aliases used for auto-detection
DATE_ALIASES    = {"date", "transaction date", "trans date", "posted", "post date", "value date"}
DESC_ALIASES    = {"description", "details", "memo", "narrative", "particulars", "transaction", "payee"}
DEBIT_ALIASES   = {"debit", "withdrawals", "withdrawal", "preauthorized wd", "charges", "payment"}
CREDIT_ALIASES  = {"credit", "deposits", "deposit", "preauthorize credit", "payments in"}
AMOUNT_ALIASES  = {"amount", "transaction amount", "net amount", "value"}
BALANCE_ALIASES = {"balance", "running balance", "closing balance"}

FOLDER_PATH = r"C:\Users\carlo\Documents\Projects\Financial Statement Parser\Credit Statement Parser\\"
DROP_ZONE = FOLDER_PATH + "drop_zone\\"
FILE_PATTERN = DROP_ZONE + "*.pdf"
LOG_PATH = FOLDER_PATH + "run_logs\\"


# ─────────────────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────────────────

def clean_amount(value) -> float | None:
    """Convert a messy amount string to a float. Returns None if unparseable."""
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    s = str(value).strip()
    if not s or s in ("-", "–"):
        return None
    negative = s.startswith("(") and s.endswith(")")
    s = re.sub(r"[^\d.\-]", "", s)
    try:
        amount = float(s)
        return -abs(amount) if negative else amount
    except ValueError:
        return None


def parse_date_str(value, statement_date=None) -> str | None:
    """Try multiple date formats and return ISO string YYYY-MM-DD or None."""
    if value is None or str(value).strip() == "":
        return None
    raw = str(value).strip()
    
    # Extract year from statement_date if provided
    year = None
    if statement_date:
        if isinstance(statement_date, str):
            match = re.search(r"\b(\d{4})\b", statement_date)
            if match:
                year = int(match.group(1))
                print(year)
        elif hasattr(statement_date, 'year'):
            year = statement_date.year
        else:
            # If it's something else, try to convert to string and search
            match = re.search(r"\b(\d{4})\b", str(statement_date))
            if match:
                year = int(match.group(1))
    # Handle MM-DD (no year) – use year from statement_date or current year
    if re.fullmatch(r"\w{3}\s+\d{1,2}", raw):
        try:
            parsed = datetime.strptime(raw, "%b %d")
            use_year = year if year else datetime.now().year
            return parsed.replace(year=use_year).strftime("%Y-%m-%d")
        except ValueError:
            pass
    formats = [
        "%m/%d/%Y", "%m/%d/%y",
        "%d/%m/%Y", "%d/%m/%y",
        "%Y-%m-%d",
        "%d-%m-%Y", "%m-%d-%Y",
        "%b %d, %Y", "%b %d %Y",
        "%B %d, %Y", "%B %d %Y",
        "%d %b %Y", "%d %B %Y",
    ]
    for fmt in formats:
        try:
            return datetime.strptime(raw, fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    return raw  # return as-is if we can't parse it


def find_column(columns: list[str], aliases: set) -> str | None:
    """Find the first column name matching a set of known aliases (case-insensitive)."""
    for col in columns:
        if col.strip().lower() in aliases:
            return col
    return None


def extract_card_last_four(text):
    """
    Extracts the card number from the given text.

    Args:
        text (str): The text to search for the card number.
    Returns:
        str or None: The extracted card number if found, otherwise None.
    """
    pattern = r" Visa Signature ending in\s+(\d{4})"
    match = re.search(pattern, text)
    if match:
        return match.group(1)
    else: 
        pass

def extract_from_text(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """
    Fallback: extract full page text and search for transaction lines using regex.
    Matches lines starting with MM-DD followed by a description and an amount.
    Example: 01-17 Preauthorized Credit VENMO CASHOUT 226.00
    """
    # Each transaction line: MM-DD  <description>  <amount>

TRANSACTION_RE = re.compile(
    r"(?m)^(\w{3}\s+\d{1,2})\s+(\w{3}\s+\d{1,2})\s+(.+?)\s+(\-?\$[\d,]+\.\d{2})$" )

def extract_statement_date(text):
    """
    Extracts the statement date from the given text.

    Args:
        text (str): The text to search for the statement date.

    Returns:
        date or None: The extracted date if found and parsed successfully, otherwise None.
    """
    pattern = r"(\w+ \d{1,2}, \d{4})\s*-\s*(\w+ \d{1,2}, \d{4})"
    match = re.search(pattern, text)
    # print(match)
    if match:
        date_str = match.group(2)
        # print(date_str)
        try:
            return datetime.strptime(date_str, "%b %d, %Y").date()
        except ValueError:
            pass
        

transactions = []
statement_date = None
cc_last_four = None
with pdfplumber.open("C:\\Users\\carlo\\Documents\\Projects\\Financial Statement Parser\\Credit Statement Parser\\drop_zone\\Statement_012025_5730.pdf") as pdf:
    for page_num, page in enumerate(pdf.pages, start=1):
        text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""
        # print(text)
        # if debug:
        #     print(f"\n  Page {page_num} raw text:\n{text}\n")
        date = extract_statement_date(text)
        last_four = extract_card_last_four(text)
        if last_four:
            cc_last_four = last_four
        # print(date)
        if date:
            statement_date = date
            # print(statement_date)
        for match in TRANSACTION_RE.finditer(text):
            # print(match.groups())
            date_raw,post_date_raw, description, amount_raw = match.groups()
            date_str    = parse_date_str(date_raw.strip(),statement_date)
            description = re.sub(r"\s{2,}", " ", description).strip()
            amount      = clean_amount(amount_raw)
            
            print(date_str, description, amount)
            if amount is None:
                continue
            # Skip descriptions that contain date patterns (e.g., "4,148.70 01-13 1,461.64 01-23")
            if re.search(r"\d{1,2}-\d{2}", description):
                continue
            
            transactions.append({
                "statement_date": statement_date,
                "transact_date":  date_str,
                "description": description,
                "amount":      amount,
                "type":        "credit" if re.search(r"(?i)credit", description) else "debit",
            })

            if debug:
                print(f"  Matched: {date_str} | {description} | {amount}")

    if not transactions:
        return pd.DataFrame()

    return pd.DataFrame(transactions).reset_index(drop=True)