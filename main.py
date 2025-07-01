from utils import sample_subset

# main.py
from pathlib import Path
from utils import sample_subset, identify_pii


def main():
    # 1. Point to your folder and CSV
    input_folder = Path("/cs/student/msc/sec/2024/rdhawan/Downloads/RDDissertation")
    input_file = input_folder / "emails.csv"

    # 2. Sample 1000 rows and get back a DataFrame
    subset_df = sample_subset(input_file, subset_size=1000, output_dir=".")

    # 3. Identify PII in that subset
    pii_df = identify_pii(subset_df, text_column='message')

    # 4. (Optional) Inspect the first few results
    print(pii_df[['message', 'pii_entities']].head())


if __name__ == "__main__":
    main()
