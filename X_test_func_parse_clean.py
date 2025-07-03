import os
import sys
import logging
import re
from pathlib import Path
import pandas as pd

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

# Constants for testing dataset files
EMAILS_TEST_CSV = "emails_with_pii.csv"
OUTPUT_CSV = "emails_with_pii_test_cleaned.csv"



def parse_email(raw: str) -> dict:
    """
    Split a raw email string into header fields and a 'Body' field.
    """
    text = raw.replace('\r\n', '\n')
    parts = text.split('\n\n', 1)
    headers, body = parts[0], parts[1] if len(parts) > 1 else ''
    data = {}
    for line in headers.split('\n'):
        if ':' in line:
            key, val = line.split(':', 1)
            data[key.strip()] = val.strip()
    data['Body'] = re.sub(r"[\t ]+", ' ', body).strip()
    return data


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace, collapse spaces/tabs, collapse runs of periods,
    and collapse extra blank lines in all string columns.
    """
    for col in df.select_dtypes(include='object').columns:
        df[col] = (
            df[col]
            .fillna('')
            .astype(str)
            .str.replace(r"[\t ]+", ' ', regex=True)
            .str.strip()
            .str.replace(r"\.{2,}", '.', regex=True)
            .str.replace(r"\n{3,}", "\n\n", regex=True)
        )
    return df

def load_pii_pipeline():
    """
    Initialize Hugging Face PII detection pipeline.
    """
    try:
        from transformers import pipeline
    except ImportError:
        logging.error("transformers library not found. Install with 'pip install transformers'.")
        sys.exit(1)

    try:
        pipe = pipeline(
            'token-classification',
            model='ab-ai/pii_model',
            aggregation_strategy='simple'
        )
        logging.info("Loaded PII pipeline: ab-ai/pii_model")
        return pipe
    except Exception as e:
        logging.exception(f"Failed to load PII pipeline: {e}")
        sys.exit(1)


def identify_pii(df: pd.DataFrame, text_column: str = 'message') -> pd.DataFrame:
    """
    Detect PII in the specified text column and add 'pii_entities' column.
    Logs progress every 100 records.
    """
    pii_pipe = load_pii_pipeline()
    pii_results = []

    for idx, text in enumerate(df[text_column]):
        try:
            result = pii_pipe(text)
        except Exception as e:
            logging.error(f"Error processing row {idx}: {e}")
            result = []

        pii_results.append(result)

        # Log progress every 100 records
        if (idx + 1) % 100 == 0:
            log_msg = f"Processed {idx + 1} of {len(df)} records"
            print(log_msg)
            logging.info(log_msg)

    df['pii_entities'] = pii_results
    logging.info(f"Finished processing PII for {len(df):,} records.")
    return df


def main():
    # 1. Load existing test CSV
    try:
        df = pd.read_csv(EMAILS_TEST_CSV, encoding='utf-8', engine='python')
        logging.info(f"Loaded test dataset: {EMAILS_TEST_CSV} ({len(df):,} rows)")
    except FileNotFoundError:
        logging.error(f"File not found: {EMAILS_TEST_CSV}")
        sys.exit(1)

    # 2. Parse raw messages into header and Body
    parsed_df = df['message'].apply(parse_email).apply(pd.Series)
    logging.info("Parsed email into header fields and Body")

    # 3. Clean all parsed text fields
    cleaned_df = clean_dataframe(parsed_df)
    logging.info("Cleaned parsed fields")

    pii_df = identify_pii(cleaned_df, text_column='Body')
    print("pii top 5 rows")
    print(pii_df[['Body', 'pii_entities']].head())

    # Save to CSV
    print("create pii csv")
    pii_df.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")
    print(f"Done. Output saved to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
