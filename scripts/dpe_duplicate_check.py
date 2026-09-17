"""One-off diagnostic (not part of the pipeline): check whether DPE records
are unique per identifiant_ban, or whether an address can have multiple
energy-diagnostic records (multiple apartments in a building, or the same
unit re-diagnosed over time). This determines whether RETL0-43's Silver
join needs a dedup/aggregation step before joining DVF to DPE."""
import pandas as pd

df = pd.read_json("data_raw/dpe/2026-09-17/75.jsonl", lines=True)
print(f"Total records: {len(df)}")
print(f"Unique identifiant_ban: {df['identifiant_ban'].nunique()}")

dup_counts = df["identifiant_ban"].value_counts()
print(f"Addresses with >1 DPE record: {(dup_counts > 1).sum()}")
print("\nTop 10 most-repeated addresses:")
print(dup_counts.head(10))