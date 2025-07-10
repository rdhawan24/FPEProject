import pandas as pd

# Step 1: Read the original CSV
input_file = "/home/roopam/PycharmProjects/FPEProject/emails_parsed_subject_grouped.csv"         # Replace with your actual filename
df = pd.read_csv(input_file)

# Step 2: Select the top 500 rows
top_500 = df.head(500)

# Step 3: Write to a new CSV file
output_file = "/home/roopam/PycharmProjects/FPEProject/top_500.csv"
top_500.to_csv(output_file, index=False)

print(f"Top 500 rows saved to {output_file}")
