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
OUTPUT_CSV = "emails_filtered_threads.csv"

# Keywords for filtering subjects
KEYWORDS = ['report', 'project', 'manager', 'status', 'update', 'approval']
MIN_MESSAGES = 3


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


def filter_threads_by_length_and_keywords(df: pd.DataFrame,
                                         min_messages: int = MIN_MESSAGES,
                                         keywords: list = KEYWORDS) -> pd.DataFrame:
    """
    Filter DataFrame to include only threads (grouped by 'Subject') that:
      - Have at least `min_messages` emails
      - Contain any of the `keywords` in the subject (case-insensitive)

    Returns the filtered DataFrame (rows belonging to matching threads).
    """
    # Group by subject and filter by thread length
    filtered = df.groupby('Subject').filter(
        lambda g: len(g) >= min_messages
    )
    # Further filter by keywords in subject
    if keywords:
        pattern = '|'.join(keywords)
        mask = filtered['Subject'].str.contains(pattern, case=False, na=False)
        filtered = filtered[mask]
    logging.info(f"Filtered to {len(filtered)} emails across qualified threads")
    return filtered


def main():
    # Load full dataset
    df = readdataset(EMAILS_SUBSET_CSV)

    # Parse into structured columns
    parsed_df = df.apply(parse_email, axis=1)
    logging.info("Parsed emails into file, X-Folder, Headers, Subject, Body")

    # Clean body
    cleaned_df = clean_dataframe(parsed_df)
    logging.info("Cleaned Body field; retained other columns")

    # Filter for multi-message threads with subject keywords
    final_df = filter_threads_by_length_and_keywords(cleaned_df)

    # Save output
    final_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    logging.info(f"Saved filtered threads to {OUTPUT_CSV}")
    print(f"Done. Output saved to {OUTPUT_CSV}")


if __name__ == '__main__':
    main()
