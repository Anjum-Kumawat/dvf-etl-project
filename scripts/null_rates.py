import pandas as pd

df = pd.read_csv("data_raw/2024/75.csv.gz", compression="gzip", low_memory=False)
null_rates = (df.isna().mean() * 100).round(1).sort_index()
print(null_rates.to_string())