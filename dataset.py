import os
import csv
import sys
import logging
from pathlib import Path
import pandas as pd
os.environ["DISSERTATION_DATA"] = "/home/roopam/Downloads/RDDissertation"




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



def main():
    # 1. Point to your folder and CSV
    #input_folder = Path(os.environ["DISSERTATION_DATA"])
    input_folder = Path(os.environ["DISSERTATION_DATA"])
    input_file = input_folder / "emails.csv"

    # 2. Sample 1000 rows and get back a DataFrame
    subset_df = sample_subset(input_file, subset_size=100, output_dir=".")
    print(subset_df.head())



# def main():
#   print("Running main...")

if __name__ == "__main__":
    main()