import os
import sys
import logging
import re
from pathlib import Path
import pandas as pd
import csv

# Set up environment (adjust if needed)
os.environ.setdefault("DISSERTATION_DATA", "/home/roopam/Downloads/RDDissertation")

# Configure logging
timestamp = '%Y-%m-%d %H:%M:%S'
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format=f'%(asctime)s %(levelname)s: %(message)s',
    datefmt=timestamp
)

# Increase CSV field size limit to handle large email bodies
csv.field_size_limit(sys.maxsize)

# Constants for dataset files
EMAILS_SUBSET_CSV = "emails_100.csv"
OUTPUT_CSV = "emails_with_pii_cleaned.csv"


def readdataset(path: str = "emails.csv") -> pd.DataFrame:
    """
    Load the Enron emails CSV into a DataFrame, handling large fields.
    Expects columns: 'file' and 'message'.
    """
    csv_path = Path(path)
    if not csv_path.is_file():
        logging.error(f"CSV file not found: {csv_path}")
        sys.exit(1)
    try:
        df = pd.read_csv(csv_path, encoding='utf-8', engine='python')
        logging.info(f"Loaded dataset from {csv_path} ({len(df):,} rows)")
        return df
    except Exception as e:
        logging.exception(f"Failed to read CSV: {e}")
        sys.exit(1)


def parse_email(row: pd.Series) -> pd.Series:
    """
    Simplified: everything before the first blank line is raw headers,
    and everything after is the body. Returns Series with 'file', 'Headers', and cleaned 'Body'.
    """
    raw = row['message']
    # Normalize line endings
    text = raw.replace('\r\n', '\n')
    # Split into headers and body on first blank line
    parts = text.split('\n\n', 1)
    headers = parts[0]
    body = parts[1] if len(parts) > 1 else ''
    # Clean body: collapse spaces/tabs and trim
    body_clean = re.sub(r'[\t ]+', ' ', body).strip()
    return pd.Series({'file': row['file'], 'Headers': headers, 'Body': body_clean})

def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace, collapse spaces/tabs, collapse runs of periods,
    and remove multiple blank lines in 'Body'; leave 'Headers' unmodified.
    """
    for col in ['Body']:
        if col in df:
            df.loc[:, col] = (
                df[col]
                .fillna('')
                .astype(str)
                .str.replace(r"[\t ]+", ' ', regex=True)
                .str.strip()
                .str.replace(r"\.{2,}", '.', regex=True)
                .str.replace(r"\n{3,}", "\n", regex=True)
            )
    return df


def main():
    # 1. Load dataset
    df = readdataset(EMAILS_SUBSET_CSV)

    # 2. Parse each row into file, Headers, Body
    parsed_df = df.apply(parse_email, axis=1)
    logging.info("Parsed email into file, Headers, and Body columns")

    # 3. Clean string columns
    cleaned_df = clean_dataframe(parsed_df)
    logging.info("Cleaned parsed fields")

    # 4. Save the output
    cleaned_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    logging.info(f"Saved cleaned parsed data to {OUTPUT_CSV}")
    print(f"Done. Output saved to {OUTPUT_CSV}")


if __name__ == '__main__':
    main()
