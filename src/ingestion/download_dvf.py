import argparse
import requests
from pathlib import Path

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"

parser = argparse.ArgumentParser()
parser.add_argument("--year", required=True)
parser.add_argument("--dept", required=True)
args = parser.parse_args()

url = BASE_URL.format(year=args.year, dept=args.dept)
dest = Path(f"data_raw/{args.year}/{args.dept}.csv.gz")
dest.parent.mkdir(parents=True, exist_ok=True)

print(f"Downloading {url}")
response = requests.get(url, stream=True)
response.raise_for_status()

with open(dest, "wb") as f:
    for chunk in response.iter_content(chunk_size=8192):
        f.write(chunk)

size_mb = dest.stat().st_size / 1024 / 1024
print(f"Saved to {dest} ({size_mb:.1f} MB)")