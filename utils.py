import os
import csv
import sys
import logging
from pathlib import Path
import pandas as pd

# Configure logging: timestamped entries to 'app.log'
logging.basicConfig(
    filename='app.log',
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Increase CSV field size limit to handle large email bodies
csv.field_size_limit(sys.maxsize)

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

    logging.info("Preview of subset: \n%s", df_sub.head().to_string())
    return df_sub


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
