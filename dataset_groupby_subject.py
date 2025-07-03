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
EMAILS_SUBSET_CSV = str(Path(os.environ["DISSERTATION_DATA"]) / "emails.csv")
OUTPUT_CSV = "emails_all_document_parsed.csv"


def readdataset(path: str) -> pd.DataFrame:
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
    Extract columns: 'file', 'X-Folder', raw 'Headers', cleaned 'Subject', and cleaned 'Body'.
    """
    raw = row['message']
    text = raw.replace('\r\n', '\n')
    parts = text.split('\n\n', 1)
    headers = parts[0]
    body = parts[1] if len(parts) > 1 else ''

    # Extract X-Folder value
    x_folder = ''
    for line in headers.split('\n'):
        if line.lower().startswith('x-folder:'):
            x_folder = line.split(':', 1)[1].strip()
            break

    # Extract Subject
    subj = ''
    for line in headers.split('\n'):
        if line.lower().startswith('subject:'):
            subj = line.split(':', 1)[1].strip()
            break
    # Remove 'Re:', 'FW:', etc.
    prefix_pattern = re.compile(r'(?i)^(?:re|fw|fwd)[:\s]+')
    while True:
        new_subj = prefix_pattern.sub('', subj).strip()
        if new_subj == subj:
            break
        subj = new_subj
    subj_clean = subj

    # Clean body
    body_clean = re.sub(r'[ \t]+', ' ', body).strip()

    return pd.Series({
        'file': row['file'],
        'X-Folder': x_folder,
        'Headers': headers,
        'Subject': subj_clean,
        'Body': body_clean
    })


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean 'Body' column: whitespace, periods, blank lines. Keep other cols as-is.
    """
    if 'Body' in df.columns:
        df.loc[:, 'Body'] = (
            df['Body']
            .fillna('')
            .astype(str)
            .str.replace(r"[ \t]+", ' ', regex=True)
            .str.strip()
            .str.replace(r"\.{2,}", '.', regex=True)
            .str.replace(r"([ \t]*\n){2,}", '\n\n', regex=True)
        )
    return df


def main():
    # Load full dataset
    df = readdataset(EMAILS_SUBSET_CSV)

    # Parse into new columns
    parsed_df = df.apply(parse_email, axis=1)
    logging.info("Parsed emails into file, X-Folder, Headers, Subject, Body")

    # Clean body
    cleaned_df = clean_dataframe(parsed_df)
    logging.info("Cleaned Body field; retained other columns")

        # Filter to 'all_documents' based on the 'file' column
    file_mask = cleaned_df['file'].str.contains(r'all_documents', case=False, na=False)
    folder_df = cleaned_df[file_mask]
    logging.info(f"Filtered to {len(folder_df)} emails with 'all_documents' in file path")

    # Exclude blank subjects
    folder_df = folder_df[folder_df['Subject'] != '']
    # Keep subjects with >3 mails
    counts = folder_df['Subject'].value_counts()
    keep = counts[counts > 3].index
    final_df = folder_df[folder_df['Subject'].isin(keep)]
    logging.info(f"Retained {len(final_df)} records after subject filtering")

    # Save output
    final_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    logging.info(f"Saved final data to {OUTPUT_CSV}")
    print(f"Done. Output saved to {OUTPUT_CSV}")

if __name__ == '__main__':
    main()
