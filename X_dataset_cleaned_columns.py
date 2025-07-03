import os
import csv
import sys
import logging
import re
from pathlib import Path
import pandas as pd


os.environ.setdefault("DISSERTATION_DATA", "/home/roopam/Downloads/RDDissertation")

# Configure logging: timestamped entries to 'app.log'
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Increase CSV field size limit to handle large email bodies
csv.field_size_limit(sys.maxsize)

EMAILS_SUBSET_CSV = "emails_100.csv"
OUTPUT_CSV = "emails_with_pii_cleaned.csv"


def readdataset(path: str = "emails.csv") -> pd.DataFrame:
    """
    Load the Enron emails CSV into a DataFrame, handling large fields.
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


def sample_subset(input_path: str, subset_size: int = 1000, output_dir: str = '.') -> pd.DataFrame:
    """
    Sample rows from the dataset and save to a CSV.
    """
    logging.info(f"Sampling {subset_size} rows from {input_path}")
    df_full = readdataset(input_path)
    df_sub = df_full.sample(n=subset_size, random_state=42)
    logging.info(f"Sampled {len(df_sub):,} rows")

    out_name = f"emails_{subset_size}.csv"
    out_path = Path(output_dir) / out_name
    try:
        df_sub.to_csv(out_path, index=False, encoding='utf-8')
        logging.info(f"Saved subset to {out_path}")
    except Exception as e:
        logging.exception(f"Failed to save subset CSV: {e}")
        sys.exit(1)

    return df_sub


import re


def parse_email(raw: str) -> dict:
    """
    Produce exactly three output columns:
      - 'file' (carried through),
      - 'Headers' (all recognized headers concatenated),
      - 'Body' (everything after the X-FileName line).
    """
    import re

    # Normalize line endings
    text = raw.replace('\r\n', '\n')

    # The headers to extract, in their original order:
    headers_order = [
        'Message-ID','Date','From','To','Subject',
        'Mime-Version','Content-Type','Content-Transfer-Encoding',
        'X-From','X-To','X-cc','X-bcc','X-Folder','X-Origin','X-FileName'
    ]

    # Build a list of "Header: value" strings
    hdrs = []
    for header in headers_order:
        # (?im) = multi-line + case-insensitive
        pattern = rf'(?im)^{header}:\s*(.*)$'
        m = re.search(pattern, text)
        val = m.group(1).strip() if m else ''
        hdrs.append(f"{header}: {val}")

    # Split off the body after the X-FileName line
    parts = re.split(r'(?im)^X-FileName:.*(?:\n|$)', text, maxsplit=1)
    body = parts[1] if len(parts) > 1 else ''
    body_clean = re.sub(r'[\t ]+', ' ', body).strip()

    # Return just Headers and Body (we'll reattach 'file' in main)
    return {
        'Headers': ' | '.join(hdrs),
        'Body': body_clean
    }



def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace, collapse multiple spaces/tabs, collapse runs of periods,
    and remove multiple blank lines in all string columns.
    """
    for col in df.select_dtypes(include='object').columns:
        df[col] = (
            df[col]
            .fillna('')
            .astype(str)
            # collapse multiple spaces or tabs
            .str.replace(r"[	 ]+", ' ', regex=True)
            .str.strip()
            # collapse runs of periods (e.g., '.....' -> '.')
            .str.replace(r"\.{2,}", '.', regex=True)
            # collapse multiple blank lines (e.g., '' -> '')
            . str.replace(r"\n{3,}", "\n\n", regex=True)
)
    return df

def main():
    # 1. Load existing test CSV
    try:
        df = pd.read_csv(EMAILS_SUBSET_CSV, encoding='utf-8', engine='python')
        logging.info(f"Loaded test dataset: {EMAILS_SUBSET_CSV} ({len(df):,} rows)")
    except FileNotFoundError:
        logging.error(f"File not found: {EMAILS_SUBSET_CSV}")
        sys.exit(1)

    # 2. Parse raw messages into header and Body
    #parsed_df = df['message'].apply(parse_email).apply(pd.Series)
    parsed = df[['file', 'message']].copy()
    parsed = parsed.join(parsed['message']
                         .apply(parse_email)
                         .apply(pd.Series)
                         )
    result = parsed[['file', 'Headers', 'Body']]

    logging.info("Parsed email into header fields and Body")

    # 3. Clean all parsed text fields
    cleaned_df = clean_dataframe(result)
    logging.info("Cleaned parsed fields")

    # 4. Save the cleaned, parsed dataset
    cleaned_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    logging.info(f"Saved cleaned parsed data to {OUTPUT_CSV}")
    print(f"Done. Output saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

