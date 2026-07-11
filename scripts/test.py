import json
import pandas as pd

# load answer key — these are the REAL duplicate customer_ids
with open("data/raw/_answer_key_duplicates.json") as f:
    truth = [json.loads(line) for line in f]

# the answer key stores the ORIGINAL customer_id that was duplicated
# the duplicate itself got a new uuid — so truth_ids are the originals
truth_original_ids = set(r["customer_id"] for r in truth)

# load processed customers
df = pd.read_parquet("data/processed/customers")

flagged_df = df[df["is_likely_duplicate"] == True]

# these are flagged but NOT in the answer key originals — false positives
false_positives = flagged_df[~flagged_df["customer_id"].isin(truth_original_ids)]

print(f"Total flagged: {len(flagged_df):,}")
print(f"False positives: {len(false_positives):,}")
print("\nSample false positive pairs (first 5):")
print(false_positives[["customer_id", "first_name", "last_name", "email", "state"]].head(5).to_string())