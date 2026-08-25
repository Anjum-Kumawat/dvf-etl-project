"""
Download one raw DVF file from data.gouv.fr.
Validates the response is non-empty and a readable gzip archive before
accepting it as a valid Bronze source. HTTP failures and archive-validation
failures both fail the command clearly with a non-zero exit code.
"""

import argparse
import sys
from pathlib import Path

import requests

sys.path.insert(0, str(Path(__file__).resolve().parent))
from validation import validate_archive

BASE_URL = "https://files.data.gouv.fr/geo-dvf/latest/csv/{year}/departements/{dept}.csv.gz"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", required=True)
    parser.add_argument("--dept", required=True)
    args = parser.parse_args()

    url = BASE_URL.format(year=args.year, dept=args.dept)
    dest = Path(f"data_raw/{args.year}/{args.dept}.csv.gz")
    dest.parent.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {url}")
    try:
        response = requests.get(url, stream=True, timeout=30)
        response.raise_for_status()
    except requests.exceptions.RequestException as exc:
        print(f"ERROR: download failed: {exc}", file=sys.stderr)
        sys.exit(1)

    with open(dest, "wb") as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)

    is_valid, reason = validate_archive(dest)
    if not is_valid:
        print(f"ERROR: downloaded file failed validation: {reason}", file=sys.stderr)
        dest.unlink(missing_ok=True)
        sys.exit(1)

    size_mb = dest.stat().st_size / 1024 / 1024
    print(f"Saved to {dest} ({size_mb:.1f} MB) — validated as a non-empty gzip archive")


if __name__ == "__main__":
    main()