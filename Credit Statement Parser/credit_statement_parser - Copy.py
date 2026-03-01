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
from glob import glob
from pathlib import Path
from datetime import datetime
import pdfplumber
import pandas as pd
# Add parent directory to sys.path so we can import local modules (e.g. db)
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

TRANSACTION_RE = re.compile(
    r"(?m)^(\w{3}\s+\d{1,2})\s+(\w{3}\s+\d{1,2})\s+(.+?)\s+(-\s*\$[\d,]+\.\d{2}|\$[\d,]+\.\d{2})$"
)

WHITESPACE_RE = re.compile(r"\s{2,}")

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
    negative = s.startswith("-") or (s.startswith("(") and s.endswith(")"))
    s = re.sub(r"[^\d.]", "", s)  # strips $, commas, spaces, dashes, parens, etc.
    try:
        amount = float(s)
        return -amount if negative else amount
    except ValueError:
        return None


def parse_date_str(value, statement_date=None) -> str | None:
    """Try multiple date formats and return ISO string YYYY-MM-DD or None.

    The parser is forgiving and will use a supplied statement_date to
    infer the year when the input lacks one (e.g. "Jan 05" or "01-05").
    If no recognized format is found, the original string is returned.
    """
    if value is None or str(value).strip() == "":
        return None

    raw = str(value).strip()

    # --- determine year context from statement_date, if available ---
    year = None
    if statement_date:
        if isinstance(statement_date, str):
            match = re.search(r"\b(\d{4})\b", statement_date)
            if match:
                year = int(match.group(1))
        elif hasattr(statement_date, 'year'):
            year = statement_date.year
        else:
            # fallback: coerce to str and re-search
            match = re.search(r"\b(\d{4})\b", str(statement_date))
            if match:
                year = int(match.group(1))

    # Special-case MMM DD (no year) and convert using context year
    if re.fullmatch(r"\w{3}\s+\d{1,2}", raw):
        try:
            parsed = datetime.strptime(raw, "%b %d")
            use_year = year if year else datetime.now().year
            return parsed.replace(year=use_year).strftime("%Y-%m-%d")
        except ValueError:
            pass

    # Try an ordered list of common date formats
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

    # nothing matched, return the original string so caller can decide
    return raw


def find_column(columns: list[str], aliases: set) -> str | None:
    """Return first column name that matches any alias (ignoring case).

    This helper is used when auto-detecting which dataframe column corresponds
    to a particular concept (date, amount, etc.).
    """
    for col in columns:
        if col.strip().lower() in aliases:
            return col
    return None

def extract_statement_date(text):
    """Search statement text for the ending date of the period.

    Many bank statements include a range such as "Jan 01, 2024 - Jan 31, 2024".
    This function returns the later (end) date which is helpful when entries
    only list month/day and the year must be inferred.
    """
    pattern = r"(\w+ \d{1,2}, \d{4})\s*-\s*(\w+ \d{1,2}, \d{4})"
    match = re.search(pattern, text)
    if match:
        date_str = match.group(2)
        try:
            return datetime.strptime(date_str, "%b %d, %Y").date()
        except ValueError:
            pass
        
def extract_card_last_four(text):
    """Pull the last four digits of a card from statement text.

    Some PDFs include a line like "Visa Signature ending in 1234" which can be
    useful for tying transactions to a specific card when multiple accounts are
    aggregated.
    """
    pattern = r" Visa Signature ending in\s+(\d{4})"
    match = re.search(pattern, text)
    if match:
        return match.group(1)

# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 1 – TABLE EXTRACTION
# ─────────────────────────────────────────────────────────────────────────────
def extract_from_tables(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """Extract transactions from embedded PDF tables.

    Many bank statements are generated with true table structure. This
    routine walks each page, collects any table it finds, and then attempts
    to infer column names and stitch all rows together.

    If debug=True the function will print information about pages and
    detected columns to aid troubleshooting.
    """
    all_rows = []
    header = None

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            tables = page.extract_tables()
            if debug:
                print(f"  Page {page_num}: found {len(tables)} table(s)")

            for table in tables:
                # ignore empty or singleton tables
                if not table or len(table) < 2:
                    continue

                # first row may be a header row
                if header is None:
                    candidate = [str(c).strip() if c else "" for c in table[0]]
                    col_set = {c.lower() for c in candidate}
                    # if header row contains a known alias we treat it as such
                    if col_set & (DATE_ALIASES | DESC_ALIASES | AMOUNT_ALIASES | DEBIT_ALIASES):
                        header = candidate
                        data_rows = table[1:]
                    else:
                        # fallback: assume first row is the header anyway
                        header = candidate
                        data_rows = table[1:]
                else:
                    # continue accumulating rows if column counts match
                    data_rows = table if len(table[0]) == len(header) else table[1:]

                for row in data_rows:
                    if row and len(row) == len(header):
                        all_rows.append(row)

    if not header or not all_rows:
        # nothing to process
        return pd.DataFrame()

    df = pd.DataFrame(all_rows, columns=header)
    df = df.dropna(how="all")  # drop completely empty rows

    if debug:
        print(f"\n  Raw columns detected: {list(df.columns)}")

    return normalize_table_df(df, debug)


def normalize_table_df(df: pd.DataFrame, debug: bool = False) -> pd.DataFrame:
    """Map auto-detected columns to standard: date, description, amount, balance.

    The inbound dataframe may have unpredictable column names; this function
    attempts to identify the relevant ones and normalize the output to a
    predictable schema.  Missing columns are simply omitted.
    """
    cols = list(df.columns)

    date_col    = find_column(cols, DATE_ALIASES)
    desc_col    = find_column(cols, DESC_ALIASES)
    debit_col   = find_column(cols, DEBIT_ALIASES)
    credit_col  = find_column(cols, CREDIT_ALIASES)
    amount_col  = find_column(cols, AMOUNT_ALIASES)
    balance_col = find_column(cols, BALANCE_ALIASES)

    if debug:
        print(f"  date→{date_col}  desc→{desc_col}  debit→{debit_col}  "
              f"credit→{credit_col}  amount→{amount_col}  balance→{balance_col}")

    out = pd.DataFrame()

    # apply conversions only if the column exists
    if date_col:
        out["date"] = df[date_col].apply(parse_date_str)

    if desc_col:
        out["description"] = df[desc_col].astype(str).str.strip()

    # determine amount and type using the best available columns
    if debit_col and credit_col:
        # separate debits and credits provided
        debits  = df[debit_col].apply(clean_amount).fillna(0)
        credits = df[credit_col].apply(clean_amount).fillna(0)
        out["amount"] = credits - debits          # positive = money in
        out["type"]   = out["amount"].apply(lambda x: "credit" if x >= 0 else "debit")
    elif amount_col:
        # single column containing signed amounts
        out["amount"] = df[amount_col].apply(clean_amount)
        out["type"]   = out["amount"].apply(
            lambda x: "credit" if x is not None and x >= 0 else "debit"
        )
    elif debit_col:
        # only debit column present, treat as negative values
        out["amount"] = df[debit_col].apply(clean_amount).apply(
            lambda x: -abs(x) if x else None
        )
        out["type"] = "debit"
    elif credit_col:
        # only credit column present; positive values
        out["amount"] = df[credit_col].apply(clean_amount)
        out["type"] = "credit"

    if balance_col:
        out["balance"] = df[balance_col].apply(clean_amount)

    # remove rows with neither date nor amount since they carry no useful info
    if "date" in out.columns and "amount" in out.columns:
        out = out.dropna(subset=["date", "amount"], how="all")

    return out.reset_index(drop=True)


# ─────────────────────────────────────────────────────────────────────────────
# STRATEGY 2 – REGEX LINE PARSING (fallback)
# ─────────────────────────────────────────────────────────────────────────────

def extract_from_text(pdf_path: str, debug: bool = False) -> pd.DataFrame:
    """Fallback text extraction when tables are not available.

    This strategy grabs all the plain text on each page and applies a
    regular expression (`TRANSACTION_RE`) to locate lines that look like
    transactions.  It is brittle but works when tables fail (e.g. in PDFs
    generated by scanners or unusual formats).
    """
    transactions = []
    statement_date = None
    cc_last_four = None

    with pdfplumber.open(pdf_path) as pdf:
        for page_num, page in enumerate(pdf.pages, start=1):
            text = page.extract_text(x_tolerance=2, y_tolerance=2) or ""

            if debug:
                print(f"\n  Page {page_num} raw text:\n{text}\n")

            # attempt to capture meta-data once per document
            statement_date = extract_statement_date(text) or statement_date
            cc_last_four = extract_card_last_four(text) or cc_last_four

            # scan for transaction-like lines
            for match in TRANSACTION_RE.finditer(text):
                date_raw, post_date_raw, description, amount_raw = match.groups()

                amount = clean_amount(amount_raw)
                if amount is None:
                    # skip lines where amount isn't parseable
                    continue

                transactions.append({
                    "cc_last_four":   cc_last_four,
                    "statement_date": statement_date,
                    "transact_date":  parse_date_str(date_raw.strip(), statement_date),
                    "post_date":      parse_date_str(post_date_raw.strip(), statement_date),
                    "description":    WHITESPACE_RE.sub(" ", description).strip(),
                    "amount":         amount,
                    "type":           "credit" if amount < 0 else "debit",
                })

                if debug:
                    print(f"  Matched: {date_raw.strip()} | {description} | {amount}")

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

    This is the high-level entry point used by both the CLI and other callers.
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"File not found: {pdf_path}")
    if path.suffix.lower() != ".pdf":
        raise ValueError(f"Expected a .pdf file, got: {pdf_path}")

    print(f"📄 Parsing: {path.name}")

    # Strategy 1: attempt to get a structured table
    print("  → Trying table extraction...")
    df = extract_from_tables(pdf_path, debug)

    if not df.empty and len(df) > 0:
        print(f"  ✅ Table extraction succeeded: {len(df)} transactions found.")
        return df

    # Strategy 2: fall back to raw text regex scanning
    print("  → Table extraction found nothing. Trying regex text parsing...")
    df = extract_from_text(pdf_path, debug)

    if not df.empty:
        print(f"  ✅ Text parsing succeeded: {len(df)} transactions found.")
    else:
        # couldn't find any transactions at all
        print("  ⚠️  No transactions found. The PDF may be scanned/image-based.")
        print("     For scanned PDFs, try: pip install pytesseract pdf2image")

    return df


def main():
    # command-line interface setup
    parser = argparse.ArgumentParser(
        description="Parse transactions from a PDF bank statement."
    )
    # the script currently scans a configured drop zone for PDFs
    parser.add_argument(
        "--debug", "-d",
        action="store_true",
        help="Print debug info (detected columns, page counts, etc.)"
    )
    args = parser.parse_args()
    files = glob(FILE_PATTERN)
    
    for file in files:
        print(f"\nProcessing file: {file}")

        try:
            df = parse_bank_statement(file, debug=args.debug)
        except (FileNotFoundError, ValueError) as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

        if df.empty:
            print("\nNo transactions extracted.")
            sys.exit(0)

        # ── Summary ─────────────────────────────────────────────────────────────
        print(f"\n{'─'*60}")
        print(f"  Total transactions : {len(df)}")
        if "type" in df.columns:
            credits = df[df["type"] == "credit"]["amount"].sum() if "amount" in df.columns else 0
            debits  = df[df["type"] == "debit"]["amount"].abs().sum() if "amount" in df.columns else 0
            # print(f"  Total credits      : {credits:>12,.2f}")
            # print(f"  Total debits       : {debits:>12,.2f}")
        if "date" in df.columns:
            dates = df["date"].dropna()
            if not dates.empty:
                print(f"  Date range         : {dates.min()}  →  {dates.max()}")
        print(f"{'─'*60}\n")

        # print(df.to_string(index=False))
        
        # # Save to database if connection is available
        # db_connection = db.Connection()
        # engine = db_connection.postgres_connect()
        # if engine is not None:
        #     df.to_sql("transactions", engine, if_exists="append", index=False)
        #     print("💾 Saved to PostgreSQL database.")

        # ── Export ───────────────────────────────────────────────────────────────
        try:
            df.to_csv(LOG_PATH, index=False)
            print(f"\n💾 Saved to: {LOG_PATH}")
        except:
            default_out = Path(file).stem + "_transactions.csv"
            df.to_csv(default_out, index=False)
            print(f"\n💾 Saved to: {default_out}")
            
        os.remove(file)


if __name__ == "__main__":
    main()
