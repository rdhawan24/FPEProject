# main.py
import pandas as pd
from pii_entities import identify_pii

# Global constant
#EMAILS_SUBSET_CSV = "emails_1000.csv"

def main():
    # 1. Load the subset CSV
    try:
        emails_df = pd.read_csv(EMAILS_SUBSET_CSV, encoding="utf-8")
    except FileNotFoundError:
        print(f"File {EMAILS_SUBSET_CSV} not found.")
        return
    print(emails_df.head())

    # 2. Identify PII in the 'message' column
    ##**pii_df = identify_pii(emails_df, text_column='message')

    # 3. Preview results
    print("pii top 5 rows")
    print(pii_df[['message', 'pii_entities']].head())

    # 4. (Optional) Save to CSV
    #print("create pii csv")
    #pii_df.to_csv("emails_with_pii.csv", index=False, encoding="utf-8")
    #print("Processed PII data saved to 'emails_with_pii.csv'.")

if __name__ == "__main__":
    main()




