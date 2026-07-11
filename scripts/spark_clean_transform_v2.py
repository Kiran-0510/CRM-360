import json
import pandas as pd

with open("data/raw/_answer_key_duplicates.json") as f:
    truth = [json.loads(line) for line in f]

# Answer key gives us BOTH sides of each duplicate pair
# A flagged customer_id is a true positive if it appears on
# EITHER side of any known duplicate pair
truth_all_ids = set()
for r in truth:
    truth_all_ids.add(r["customer_id"])        # the injected duplicate
    truth_all_ids.add(r["is_duplicate_of"])    # the original it was copied from

df = pd.read_parquet("data/processed/customers")
flagged = set(df[df["is_likely_duplicate"] == True]["customer_id"].tolist())

true_positives  = len(flagged & truth_all_ids)
false_positives = len(flagged - truth_all_ids)
false_negatives = len(truth_all_ids - flagged)

print(f"True positives:  {true_positives:,}")
print(f"False positives: {false_positives:,}")
print(f"False negatives: {false_negatives:,}")
print(f"Precision: {true_positives / max(true_positives + false_positives, 1):.2%}")
print(f"Recall:    {true_positives / max(true_positives + false_negatives, 1):.2%}")