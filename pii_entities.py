
import re
import os
import csv
import sys
import logging
from pathlib import Path
import pandas as pd
EMAILS_SUBSET_CSV = "emails_100.csv"

# Configure logging: timestamped entries to 'app.log'
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

def extract_body(text: str) -> str:
    """
    Given the full raw email in `text`, return everything after the
    first line that begins with 'X-FileName:' (i.e. the message body).
    """
    # Regex splits on the X-FileName line (including the newline)
    parts = re.split(r"(?im)^X-FileName:.*\r?\n", text, maxsplit=1)
    # parts[0] is header, parts[1] is the body (if present)
    if len(parts) > 1:
        return parts[1].strip()
    else:
        # fallback: no X-FileName found
        return text.strip()

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
    Detect PII in the specified text column and add 'pii_entities'.
    """
    pii_pipe = load_pii_pipeline()
    try:
        df['pii_entities'] = df[text_column].apply(lambda txt: pii_pipe(txt))
        logging.info(f"Identified PII in column '{text_column}' for {len(df):,} rows")
        return df
    except Exception as e:
        logging.exception(f"Failed to identify PII: {e}")
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
    # 1. Load the subset CSV
    try:
        emails_df = pd.read_csv(EMAILS_SUBSET_CSV, encoding="utf-8")
    except FileNotFoundError:
        print(f"File {EMAILS_SUBSET_CSV} not found.")
        return
    print(emails_df.head())

    # new column "body" with only the content after X-FileName
    emails_df['body'] = emails_df['message'].apply(extract_body)

    # Identify PII in the 'message' column
    pii_df = identify_pii(emails_df, text_column='body')

    # Preview results
    print("pii top 5 rows")
    print(pii_df[['body', 'pii_entities']].head())

    # Save to CSV
    print("create pii csv")
    pii_df.to_csv("emails_with_pii.csv", index=False, encoding="utf-8")
    print("Processed PII data saved to 'emails_with_pii.csv'.")

if __name__ == "__main__":
    main()
