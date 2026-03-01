#!/usr/bin/env python3
"""
PDF Bank Statement Transaction Parser
======================================
Extracts transactions from a PDF bank statement using two strategies:
  1. Table extraction  – works when the PDF has embedded table structure
  2. Regex line parsing – fallback for text-based PDFs with no tables

Requirements:
    pip install pdfplumber pandas

Usage:
    python parse_bank_statement_pdf.py statement.pdf
    python parse_bank_statement_pdf.py statement.pdf --output transactions.csv
    python parse_bank_statement_pdf.py statement.pdf --debug
"""

import re
import sys
import os
import argparse
import pdfplumber
import pandas as pd
import shutil
import logging
from glob import glob
from pathlib import Path
from datetime import datetime
# Add parent directory to sys.path
parent_dir = Path(__file__).parent.parent
sys.path.insert(0, str(parent_dir))
import db

# configure global logger; default level INFO, timestamped messages
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


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

FOLDER_PATH = r"C:\Users\carlo\Documents\Projects\Financial Statement Parser\Bank Statement Parser\\"
DROP_ZONE = FOLDER_PATH + "drop_zone\\"
FILE_PATTERN = DROP_ZONE + "*.pdf"
LOG_PATH = FOLDER_PATH + "run_logs\\"
ARCHIVE_PATH = FOLDER_PATH + "archive\\"

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
        elif hasattr(statement_date, 'year'):
            year = statement_date.year
        else:
            # If it's something else, try to convert to string and search
            match = re.search(r"\b(\d{4})\b", str(statement_date))
            if match:
                year = int(match.group(1))

    # Handle MM-DD (no year) – use year from statement_date or current year
    if re.fullmatch(r"\d{1,2}-\d{1,2}", raw):
        try:
            parsed = datetime.strptime(raw, "%m-%d")
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


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 1 – TABLE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_tables(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """
    Extract transactions from embedded PDF tables.
    Tries to auto-detect date, description, amount/debit/credit columns.
    """
    all_rows = []
    header = None

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if debug:
                logger.debug(f"  Page {page_num}: found {len(tables)} table(s)")

            for table in tables:
                if not table or len(table) < 2:
                    continue

                # Use first row as header if we don't have one yet
                if header is None:
                    candidate = [str(c).strip() if c else "" for c in table[0]]
                    # Check if this looks like a header (contains a known alias)
                    col_set = {c.lower() for c in candidate}
                    if col_set & (DATE_ALIASES | DESC_ALIASES | AMOUNT_ALIASES | DEBIT_ALIASES):
                        header = candidate
                        data_rows = table[1:]
                    else:
                        header = candidate  # assume it is anyway
                        data_rows = table[1:]
                else:
                    # If same column count, continue accumulating rows
                    data_rows = table if len(table[0]) == len(header) else table[1:]

                for row in data_rows:
                    if row and len(row) == len(header):
                        all_rows.append(row)

    if not header or not all_rows:
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=header)
    df = df.dropna(how="all")

    if debug:
        logger.debug(f"\n  Raw columns detected: {list(df.columns)}")

    return normalize_table_df(df, debug)


def normalize_table_df(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """Map auto-detected columns to standard: date, description, amount, balance."""
    cols = list(df.columns)

    date_col    = find_column(cols, DATE_ALIASES)
    desc_col    = find_column(cols, DESC_ALIASES)
    debit_col   = find_column(cols, DEBIT_ALIASES)
    credit_col  = find_column(cols, CREDIT_ALIASES)
    amount_col  = find_column(cols, AMOUNT_ALIASES)
    balance_col = find_column(cols, BALANCE_ALIASES)

    if debug:
        logger.debug(f"  date→{date_col}  desc→{desc_col}  debit→{debit_col}  "
              f"credit→{credit_col}  amount→{amount_col}  balance→{balance_col}")

    out = pd.DataFrame()

    if date_col:
        out["date"] = df[date_col].apply(parse_date_str)

    if desc_col:
        out["description"] = df[desc_col].astype(str).str.strip()

    # Resolve amount: prefer separate debit/credit cols, else single amount col
    if debit_col and credit_col:
        debits  = df[debit_col].apply(clean_amount).fillna(0)
        credits = df[credit_col].apply(clean_amount).fillna(0)
        out["amount"] = credits - debits          # positive = money in
        out["type"]   = out["amount"].apply(lambda x: "credit" if x >= 0 else "debit")
    elif amount_col:
        out["amount"] = df[amount_col].apply(clean_amount)
        out["type"]   = out["amount"].apply(
            lambda x: "credit" if x is not None and x >= 0 else "debit"
        )
    elif debit_col:
        out["amount"] = df[debit_col].apply(clean_amount).apply(
            lambda x: -abs(x) if x else None
        )
        out["type"] = "debit"
    elif credit_col:
        out["amount"] = df[credit_col].apply(clean_amount)
        out["type"] = "credit"

    if balance_col:
        out["balance"] = df[balance_col].apply(clean_amount)

    # Drop rows with no date AND no amount
    if "date" in out.columns and "amount" in out.columns:
        out = out.dropna(subset=["date", "amount"], how="all")

    return out.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 2 – REGEX LINE PARSING (fallback)
# ─────────────────────────────────────────────────────────────────────────────

def extract_statement_date(text):
    """
    Extracts the statement date from the given text.

    Args:
        text (str): The text to search for the statement date.

    Returns:
        date or None: The extracted date if found and parsed successfully, otherwise None.
    """
    pattern = r'This statement:\s+(\w+ \d{1,2}, \d{4})'
    match = re.search(pattern, text)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, "%B %d, %Y").date()
        except ValueError:
            pass

def extract_from_text(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """
    Fallback: extract full page text and search for transaction lines using regex.
    Matches lines starting with MM-DD followed by a description and an amount.
    Example: 01-17 Preauthorized Credit VENMO CASHOUT 226.00
    """
    # Each transaction line: MM-DD  <description>  <amount>
    TRANSACTION_RE = re.compile(
        r"(?m)^(\d{1,2}-\d{2})\s+(.+?)\s+([\(\-]?\$?[\d,]+\.\d{2}\)?)$"
    )

    transactions = []
    statement_date = None
    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""

            if debug:
                logger.debug(f"\n  Page {page_num} raw text:\n{text}\n")

            date = extract_statement_date(text)
            if date:
                statement_date = date
            
            for match in TRANSACTION_RE.finditer(text):
                date_raw, description, amount_raw = match.groups()

                date_str    = parse_date_str(date_raw.strip(),statement_date)
                description = re.sub(r"\s{2,}", " ", description).strip()
                amount      = clean_amount(amount_raw)

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
                    logger.debug(f"  Matched: {date_str} | {description} | {amount}")

    if not transactions:
        return pd.DataFrame()

    return pd.DataFrame(transactions).reset_index(drop=True)

        
# ─────────────────────────────────────────────────────────────────────────────
# MAIN ENTRY POINT
# ─────────────────────────────────────────────────────────────────────────────

def parse_bank_statement(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """
    Parse a PDF bank statement. Tries table extraction first, then regex fallback.
    Returns a DataFrame with columns: date, description, amount, type, balance (where available).
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {pdf_path}")
    if path.suffix.lower() != ".pdf":
        raise ValueError(f"Expected a .pdf file, got: {pdf_path}")

    logger.info(f"📄 Parsing: {path.name}")

    # ── Strategy 1: table extraction ────────────────────────────────────────
    logger.info("  → Trying table extraction...")
    df = extract_from_tables(pdf_path, debug)

    if not df.empty and len(df) > 0:
        logger.info(f"  ✅ Table extraction succeeded: {len(df)} transactions found.")
        return df

    # ── Strategy 2: regex line parsing ──────────────────────────────────────
    logger.info("  → Table extraction found nothing. Trying regex text parsing...")
    df = extract_from_text(pdf_path, debug)

    if not df.empty:
        logger.info(f"  ✅ Text parsing succeeded: {len(df)} transactions found.")
    else:
        logger.warning("  ⚠️  No transactions found. The PDF may be scanned/image-based.")
        logger.info("     For scanned PDFs, try: pip install pytesseract pdf2image")

    return df


def main():
    parser = argparse.ArgumentParser(
        description="Parse transactions from a PDF bank statement."
    )
    # parser.add_argument("pdf_file", help="Path to the PDF bank statement")
    # parser.add_argument(
    #     "--output", "-o",
    #     default=None,
    #     help="Save results to this CSV file (optional)"
    # )
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Print debug info (detected columns, page counts, etc.)"
    )
    args = parser.parse_args()
    if args.debug:
        logger.setLevel(logging.DEBUG)
    files = glob(FILE_PATTERN)
    
    for file in files:

        try:
            df = parse_bank_statement(file, debug=args.debug)
        except (FileNotFoundError, ValueError) as e:
            logger.error(f"Error: {e}")
            sys.exit(1)

        if df.empty:
            logger.info("\nNo transactions extracted.")
            sys.exit(0)

        # ── Summary ─────────────────────────────────────────────────────────────
        logger.info(f"\n{'─'*60}")
        logger.info(f"  Total transactions : {len(df)}")
        if "type" in df.columns:
            credits = df[df["type"] == "credit"]["amount"].sum() if "amount" in df.columns else 0
            debits  = df[df["type"] == "debit"]["amount"].abs().sum() if "amount" in df.columns else 0
            # logger.info(f"  Total credits      : {credits:>12,.2f}")
            # logger.info(f"  Total debits       : {debits:>12,.2f}")
        if "date" in df.columns:
            dates = df["date"].dropna()
            if not dates.empty:
                logger.info(f"  Date range         : {dates.min()}  →  {dates.max()}")
        logger.info(f"{'─'*60}\n")

        # logger.info(df.to_string(index=False))
        
        # Save to database if connection is available
        db_connection = db.Connection()
        engine = db_connection.postgres_connect()
        if engine is not None:
            df.to_sql("synovus", engine,schema="statements", if_exists="append", index=False)
            logger.info("💾 Saved to PostgreSQL database.")

        # ── Export ───────────────────────────────────────────────────────────────
        try:
            df.to_csv(LOG_PATH, index=False)
            logger.info(f"\n💾 Saved to: {LOG_PATH}")
        except:
            default_out = Path(file).stem + "_transactions.csv"
            df.to_csv(default_out, index=False)
            logger.info(f"\n💾 Saved to: {default_out}")
            
        if not os.path.exists(ARCHIVE_PATH):
            os.makedirs(ARCHIVE_PATH)
        shutil.move(file, ARCHIVE_PATH)
        logger.info(f"📁 Moved original PDF to archive: {ARCHIVE_PATH}")


if __name__ == "__main__":
    main()
