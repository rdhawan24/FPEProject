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
EMAILS_SUBSET_CSV = "emails_1000.csv"
OUTPUT_CSV = "emails_with_pii_validated.csv"


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
    Everything before the first blank line is raw headers,
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
    body_clean = re.sub(r'[ \t]+', ' ', body).strip()
    return pd.Series({'file': row['file'], 'Headers': headers, 'Body': body_clean})


def clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """
    Trim whitespace, collapse spaces/tabs, collapse runs of periods,
    and collapse multiple blank lines in 'Body'; leave 'Headers' unmodified.
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


def identify_pii(df: pd.DataFrame, text_column: str = 'Body') -> pd.DataFrame:
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

        if (idx + 1) % 100 == 0:
            log_msg = f"Processed {idx + 1} of {len(df)} records"
            print(log_msg)
            logging.info(log_msg)

    df = df.copy()
    df['pii_entities'] = pii_results
    logging.info(f"Finished processing PII for {len(df):,} records.")
    return df


# --- Validation utilities ---

def firstname_validate(name: str) -> bool:
    """
    Return True if `name`:
      - Does NOT start with punctuation or symbols (@#\$& etc.)
      - Has at least two letters (A–Z or a–z)
      - May contain internal hyphens or apostrophes
      - May have trailing punctuation, which is ignored
    """
    # 1) Strip trailing punctuation .,;:!?
    clean = re.sub(r"[.,;:!?]+$", "", name)

    # 2) Match: start with letter, then at least one more letter,
    #    then optionally internal groups of - or ' plus letters
    pattern = r"^[A-Za-z][A-Za-z]+(?:[-'][A-Za-z]+)*$"
    return bool(re.fullmatch(pattern, clean))


def lastname_validate(name: str) -> bool:
    """
    Same rules as for first name.
    """
    clean = re.sub(r"[.,;:!?]+$", "", name)
    pattern = r"^[A-Za-z][A-Za-z]+(?:[-'][A-Za-z]+)*$"
    return bool(re.fullmatch(pattern, clean))


def validate_entities(df: pd.DataFrame, entities_col: str = "pii_entities") -> pd.DataFrame:
    """
    For each row, look at the list of dicts in `entities_col`,
    add a `valid` flag to *every* entity, and keep them all.
    """
    all_validated = []
    for ents in df[entities_col]:
        row_validated = []
        for ent in ents:
            grp  = ent.get("entity_group", "")
            word = ent.get("word", "")
            if grp == "FIRSTNAME":
                ent["valid"] = firstname_validate(word)
            elif grp == "LASTNAME":
                ent["valid"] = lastname_validate(word)
            else:
                ent["valid"] = True
            row_validated.append(ent)   # <— always append, valid or not
        all_validated.append(row_validated)

    # Make a copy, then assign the new column
    df = df.copy()
    df["pii_entities_validated"] = all_validated
    return df


def main():
    # 1. Load dataset
    df = readdataset(EMAILS_SUBSET_CSV)

    # 2. Parse into file, Headers, Body
    parsed_df = df.apply(parse_email, axis=1)
    logging.info("Parsed emails into file, Headers, and Body columns")

    # 3. Clean body only
    cleaned_df = clean_dataframe(parsed_df)
    logging.info("Cleaned Body field; retained Headers")

    # 4. Identify PII
    pii_df = identify_pii(cleaned_df, text_column='Body')

    # 5. Validate firstname/lastname entities
    validated_df = validate_entities(pii_df, entities_col='pii_entities')
    logging.info("Validated FIRSTNAME/LASTNAME entities")

    # 6. Save output
    validated_df.to_csv(OUTPUT_CSV, index=False, encoding='utf-8')
    logging.info(f"Saved PII-validated data to {OUTPUT_CSV}")
    print(f"Done. Output saved to {OUTPUT_CSV}")


if __name__ == '__main__':
    main()
